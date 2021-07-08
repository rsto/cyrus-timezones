/* FIXME
 *
 * license
 *
 */

#include <errno.h>
#include <fcntl.h>
#include <fts.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>
#include <syslog.h>
#include <unistd.h>

#include <jansson.h>
#include <libical/ical.h>

/*
 *   file magic: <cstring 'cyr_guesstz', including zero>
 *
 *   dbVersion: <uint8_t>
 *     The database version.
 *
 *   createdAt: <int64_t>
 *     The datetime this database was created at, in UNIX epoch time.
 *
 *   timeRange:
 *     The time range in which timezone observances are expanded,
 *     in UNIX epoch time.
 *     start: <int64_t>
 *       The start of the time range.
 *     end: <int64_t>
 *       The start of the time range.
 *
 *   ianaVersion: <cstring, including zero>
 *     The IANA database version this database is based on.
 *
 *   offsets_cnt: <uint32_t>
 *
 *   offsets_cnt offsets, sorted ascending by offset:
 *       offset: int32_t
 *       tzindex: uint64_t
 *
 *   zero or more timezones:
 *       tzid: <cstring, including zero>
 *         The timezone identifier of the timezone.
 *
 *       observances_cnt: <uint32_t>
 *
 *       observances_cnt observances, sorted ascending by onset:
 *         onset: <int64_t>
 *           The onset time of the observance, in UNIX epoch time.
 *         offset: <int32_t>
 *           The UTC offset of the observance in seconds.
 *
 */

struct icalobservance {
    const char *name;
    icaltimetype onset;
    int offset_from;
    int offset_to;
    int is_daylight;
    int is_std;
    int is_gmt;
};

static void icaltime_set_utc(struct icaltimetype *t, int set)
{
    icaltime_set_timezone(t, set ? icaltimezone_get_utc_timezone() : NULL);
}

static void get_isstd_isgmt(icalproperty *prop,
                                         struct icalobservance *obs)
{
    const char *time_type =
        icalproperty_get_parameter_as_string(prop, "X-OBSERVED-AT");

    if (!time_type) time_type = "W";

    switch (time_type[0]) {
    case 'G': case 'g':
    case 'U': case 'u':
    case 'Z': case 'z':
        obs->is_gmt = obs->is_std = 1;
        break;
    case 'S': case 's':
        obs->is_gmt = 0;
        obs->is_std = 1;
        break;
    case 'W': case 'w':
    default:
        obs->is_gmt = obs->is_std = 0;
        break;
    }
}

static void check_tombstone(struct icalobservance *tombstone,
                            struct icalobservance *obs)
{
    if (icaltime_compare(obs->onset, tombstone->onset) > 0) {
        /* onset is closer to cutoff than existing tombstone */
        tombstone->name = icalmemory_tmp_copy(obs->name);
        tombstone->offset_from = tombstone->offset_to = obs->offset_to;
        tombstone->is_daylight = obs->is_daylight;
        tombstone->onset = obs->onset;
    }
}

struct rdate {
    icalproperty *prop;
    struct icaldatetimeperiodtype date;
};

static int rdate_compare(const void *rdate1, const void *rdate2)
{
    return icaltime_compare(((struct rdate *) rdate1)->date.time,
                            ((struct rdate *) rdate2)->date.time);
}

static int observance_compare(const void *obs1, const void *obs2)
{
    return icaltime_compare(((struct icalobservance *) obs1)->onset,
                            ((struct icalobservance *) obs2)->onset);
}

static void truncate_vtimezone_advanced(icalcomponent *vtz,
                                        icaltimetype *startp, icaltimetype *endp,
                                        icalarray *obsarray,
                                        struct icalobservance **proleptic,
                                        icalcomponent **eternal_std,
                                        icalcomponent **eternal_dst,
                                        icaltimetype *last_dtstart,
                                        int ms_compatible)
{
    icaltimetype start = *startp, end = *endp;
    icalcomponent *comp, *nextc, *tomb_std = NULL, *tomb_day = NULL;
    icalproperty *prop, *proleptic_prop = NULL;
    struct icalobservance tombstone;
    unsigned need_tomb = !icaltime_is_null_time(start);
    unsigned adjust_start = !icaltime_is_null_time(start);
    unsigned adjust_end = !icaltime_is_null_time(end);

    if (last_dtstart) *last_dtstart = icaltime_null_time();

    /* See if we have a proleptic tzname in VTIMEZONE */
    for (prop = icalcomponent_get_first_property(vtz, ICAL_X_PROPERTY);
         prop;
         prop = icalcomponent_get_next_property(vtz, ICAL_X_PROPERTY)) {
        if (!strcmp("X-PROLEPTIC-TZNAME", icalproperty_get_x_name(prop))) {
            proleptic_prop = prop;
            break;
        }
    }

    memset(&tombstone, 0, sizeof(struct icalobservance));
    tombstone.name = icalmemory_tmp_copy(proleptic_prop ?
                                         icalproperty_get_x(proleptic_prop) :
                                         "LMT");
    if (!proleptic_prop ||
        !icalproperty_get_parameter_as_string(prop, "X-NO-BIG-BANG"))
      tombstone.onset.year = -1;

    /* Process each VTMEZONE STANDARD/DAYLIGHT subcomponent */
    for (comp = icalcomponent_get_first_component(vtz, ICAL_ANY_COMPONENT);
         comp; comp = nextc) {
        icalproperty *dtstart_prop = NULL, *rrule_prop = NULL;
        icalarray *rdate_array = icalarray_new(sizeof(struct rdate), 10);
        icaltimetype dtstart;
        struct icalobservance obs;
        unsigned n, trunc_dtstart = 0;
        int r;

        nextc = icalcomponent_get_next_component(vtz, ICAL_ANY_COMPONENT);

        memset(&obs, 0, sizeof(struct icalobservance));
        obs.offset_from = obs.offset_to = INT_MAX;
        obs.is_daylight = (icalcomponent_isa(comp) == ICAL_XDAYLIGHT_COMPONENT);

        /* Grab the properties that we require to expand recurrences */
        for (prop = icalcomponent_get_first_property(comp, ICAL_ANY_PROPERTY);
             prop;
             prop = icalcomponent_get_next_property(comp, ICAL_ANY_PROPERTY)) {

            switch (icalproperty_isa(prop)) {
            case ICAL_TZNAME_PROPERTY:
                obs.name = icalproperty_get_tzname(prop);
                break;

            case ICAL_DTSTART_PROPERTY:
                dtstart_prop = prop;
                obs.onset = dtstart = icalproperty_get_dtstart(prop);
                get_isstd_isgmt(prop, &obs);
                if (last_dtstart && icaltime_compare(dtstart, *last_dtstart))
                    *last_dtstart = dtstart;
                break;

            case ICAL_TZOFFSETFROM_PROPERTY:
                obs.offset_from = icalproperty_get_tzoffsetfrom(prop);
                break;

            case ICAL_TZOFFSETTO_PROPERTY:
                obs.offset_to = icalproperty_get_tzoffsetto(prop);
                break;

            case ICAL_RRULE_PROPERTY:
                rrule_prop = prop;
                break;

            case ICAL_RDATE_PROPERTY: {
                struct rdate rdate = { prop, icalproperty_get_rdate(prop) };

                icalarray_append(rdate_array, &rdate);
                break;
            }

            default:
                /* ignore all other properties */
                break;
            }
        }

        /* We MUST have DTSTART, TZNAME, TZOFFSETFROM, and TZOFFSETTO */
        if (!dtstart_prop || !obs.name ||
            obs.offset_from == INT_MAX || obs.offset_to == INT_MAX) {
            icalarray_free(rdate_array);
            continue;
        }

        /* Adjust DTSTART observance to UTC */
        icaltime_adjust(&obs.onset, 0, 0, 0, -obs.offset_from);
        icaltime_set_utc(&obs.onset, 1);

        /* Check DTSTART vs window close */
        if (!icaltime_is_null_time(end) &&
            icaltime_compare(obs.onset, end) >= 0) {
            /* All observances occur on/after window close - remove component */
            icalcomponent_remove_component(vtz, comp);
            icalcomponent_free(comp);

            /* Actual range end == request range end */
            adjust_end = 0;

            /* Nothing else to do */
            icalarray_free(rdate_array);
            continue;
        }

        /* Check DTSTART vs window open */
        r = icaltime_compare(obs.onset, start);
        if (r < 0) {
            /* DTSTART is prior to our window open - check it vs tombstone */
            if (need_tomb) check_tombstone(&tombstone, &obs);

            /* Adjust it */
            trunc_dtstart = 1;

            /* Actual range start == request range start */
            adjust_start = 0;
        }
        else {
            /* DTSTART is on/after our window open */
            if (r == 0) need_tomb = 0;

            if (obsarray && !rrule_prop) {
                /* Add the DTSTART observance to our array */
                icalarray_append(obsarray, &obs);
            }
        }

        if (rrule_prop) {
            struct icalrecurrencetype rrule =
                icalproperty_get_rrule(rrule_prop);
            icalrecur_iterator *ritr = NULL;
            unsigned eternal = icaltime_is_null_time(rrule.until);
            unsigned trunc_until = 0;

            if (eternal) {
                if (obs.is_daylight) {
                    if (eternal_dst) *eternal_dst = comp;
                }
                else if (eternal_std) *eternal_std = comp;
            }

            /* Check RRULE duration */
            if (!eternal && icaltime_compare(rrule.until, start) < 0) {
                /* RRULE ends prior to our window open -
                   check UNTIL vs tombstone */
                obs.onset = rrule.until;
                if (need_tomb) check_tombstone(&tombstone, &obs);

                /* Remove RRULE */
                icalcomponent_remove_property(comp, rrule_prop);
                icalproperty_free(rrule_prop);
            }
            else {
                /* RRULE ends on/after our window open */
                if (!icaltime_is_null_time(end) &&
                    (eternal || icaltime_compare(rrule.until, end) >= 0)) {
                    /* RRULE ends after our window close - need to adjust it */
                    trunc_until = 1;
                }

                if (!eternal) {
                    /* Adjust UNTIL to local time (for iterator) */
                    icaltime_adjust(&rrule.until, 0, 0, 0, obs.offset_from);
                    icaltime_set_utc(&rrule.until, 0);
                }

                if (trunc_dtstart) {
                    /* Bump RRULE start to 1 year prior to our window open */
                    dtstart.year = start.year - 1;
                    dtstart.month = start.month;
                    dtstart.day = start.day;
                    icaltime_normalize(dtstart);
                }

                ritr = icalrecur_iterator_new(rrule, dtstart);
            }

            /* Process any RRULE observances within our window */
            if (ritr) {
                icaltimetype recur, prev_onset;

                /* Mark original DTSTART (UTC) */
                dtstart = obs.onset;

                while (!icaltime_is_null_time(obs.onset = recur =
                                              icalrecur_iterator_next(ritr))) {
                    unsigned ydiff;

                    /* Adjust observance to UTC */
                    icaltime_adjust(&obs.onset, 0, 0, 0, -obs.offset_from);
                    icaltime_set_utc(&obs.onset, 1);

                    if (trunc_until && icaltime_compare(obs.onset, end) >= 0) {
                        /* Observance is on/after window close */

                        /* Actual range end == request range end */
                        adjust_end = 0;

                        /* Check if DSTART is within 1yr of prev onset */
                        ydiff = prev_onset.year - dtstart.year;
                        if (ydiff <= 1) {
                            /* Remove RRULE */
                            icalcomponent_remove_property(comp, rrule_prop);
                            icalproperty_free(rrule_prop);

                            if (ydiff) {
                                /* Add previous onset as RDATE */
                                struct icaldatetimeperiodtype rdate = {
                                    prev_onset,
                                    icalperiodtype_null_period()
                                };
                                prop = icalproperty_new_rdate(rdate);
                                icalcomponent_add_property(comp, prop);
                            }
                        }
                        else if (!eternal) {
                            /* Set UNTIL to previous onset */
                            rrule.until = prev_onset;
                            icalproperty_set_rrule(rrule_prop, rrule);
                        }

                        /* We're done */
                        break;
                    }

                    /* Check observance vs our window open */
                    r = icaltime_compare(obs.onset, start);
                    if (r < 0) {
                        /* Observance is prior to our window open -
                           check it vs tombstone */
                        if (ms_compatible) {
                            /* XXX  We don't want to move DTSTART of the RRULE
                               as Outlook/Exchange doesn't appear to like
                               truncating the frontend of RRULEs */
                            need_tomb = 0;
                            trunc_dtstart = 0;
                            if (proleptic_prop) {
                                icalcomponent_remove_property(vtz,
                                                              proleptic_prop);
                                icalproperty_free(proleptic_prop);
                                proleptic_prop = NULL;
                            }
                        }
                        if (need_tomb) check_tombstone(&tombstone, &obs);
                    }
                    else {
                        /* Observance is on/after our window open */
                        if (r == 0) need_tomb = 0;

                        if (trunc_dtstart) {
                            /* Make this observance the new DTSTART */
                            icalproperty_set_dtstart(dtstart_prop, recur);
                            dtstart = obs.onset;
                            trunc_dtstart = 0;

                            if (last_dtstart &&
                                icaltime_compare(dtstart, *last_dtstart) > 0) {
                                *last_dtstart = dtstart;
                            }

                            /* Check if new DSTART is within 1yr of UNTIL */
                            ydiff = rrule.until.year - recur.year;
                            if (!trunc_until && ydiff <= 1) {
                                /* Remove RRULE */
                                icalcomponent_remove_property(comp, rrule_prop);
                                icalproperty_free(rrule_prop);

                                if (ydiff) {
                                    /* Add UNTIL as RDATE */
                                    struct icaldatetimeperiodtype rdate = {
                                        rrule.until,
                                        icalperiodtype_null_period()
                                    };
                                    prop = icalproperty_new_rdate(rdate);
                                    icalcomponent_add_property(comp, prop);
                                }
                            }
                        }

                        if (obsarray) {
                            /* Add the observance to our array */
                            icalarray_append(obsarray, &obs);
                        }
                        else if (!trunc_until) {
                            /* We're done */
                            break;
                        }
                    }
                    prev_onset = obs.onset;
                }
                icalrecur_iterator_free(ritr);
            }
        }

        /* Sort the RDATEs by onset */
        icalarray_sort(rdate_array, &rdate_compare);

        /* Check RDATEs */
        for (n = 0; n < rdate_array->num_elements; n++) {
            struct rdate *rdate = icalarray_element_at(rdate_array, n);

            if (n == 0 && icaltime_compare(rdate->date.time, dtstart) == 0) {
                /* RDATE is same as DTSTART - remove it */
                icalcomponent_remove_property(comp, rdate->prop);
                icalproperty_free(rdate->prop);
                continue;
            }

            obs.onset = rdate->date.time;
            get_isstd_isgmt(rdate->prop, &obs);

            /* Adjust observance to UTC */
            icaltime_adjust(&obs.onset, 0, 0, 0, -obs.offset_from);
            icaltime_set_utc(&obs.onset, 1);

            if (!icaltime_is_null_time(end) &&
                icaltime_compare(obs.onset, end) >= 0) {
                /* RDATE is after our window close - remove it */
                icalcomponent_remove_property(comp, rdate->prop);
                icalproperty_free(rdate->prop);

                /* Actual range end == request range end */
                adjust_end = 0;

                continue;
            }

            r = icaltime_compare(obs.onset, start);
            if (r < 0) {
                /* RDATE is prior to window open - check it vs tombstone */
                if (need_tomb) check_tombstone(&tombstone, &obs);

                /* Remove it */
                icalcomponent_remove_property(comp, rdate->prop);
                icalproperty_free(rdate->prop);

                /* Actual range start == request range start */
                adjust_start = 0;
            }
            else {
                /* RDATE is on/after our window open */
                if (r == 0) need_tomb = 0;

                if (trunc_dtstart) {
                    /* Make this RDATE the new DTSTART */
                    icalproperty_set_dtstart(dtstart_prop,
                                             rdate->date.time);
                    trunc_dtstart = 0;

                    icalcomponent_remove_property(comp, rdate->prop);
                    icalproperty_free(rdate->prop);
                }

                if (obsarray) {
                    /* Add the observance to our array */
                    icalarray_append(obsarray, &obs);
                }
            }
        }
        icalarray_free(rdate_array);

        /* Final check */
        if (trunc_dtstart) {
            /* All observances in comp occur prior to window open, remove it
               unless we haven't saved a tombstone comp of this type yet */
            if (icalcomponent_isa(comp) == ICAL_XDAYLIGHT_COMPONENT) {
                if (!tomb_day) {
                    tomb_day = comp;
                    comp = NULL;
                }
            }
            else if (!tomb_std) {
                tomb_std = comp;
                comp = NULL;
            }

            if (comp) {
                icalcomponent_remove_component(vtz, comp);
                icalcomponent_free(comp);
            }
        }
    }

    if (need_tomb && !icaltime_is_null_time(tombstone.onset)) {
        /* Need to add tombstone component/observance starting at window open
           as long as its not prior to start of TZ data */
        icalcomponent *tomb;
        icalproperty *prop, *nextp;

        if (obsarray) {
            /* Add the tombstone to our array */
            tombstone.onset = start;
            tombstone.is_gmt = tombstone.is_std = 1;
            icalarray_append(obsarray, &tombstone);
        }

        /* Determine which tombstone component we need */
        if (tombstone.is_daylight) {
            tomb = tomb_day;
            tomb_day = NULL;
        }
        else {
            tomb = tomb_std;
            tomb_std = NULL;
        }

        /* Set property values on our tombstone */
        for (prop = icalcomponent_get_first_property(tomb, ICAL_ANY_PROPERTY);
             prop; prop = nextp) {

            nextp = icalcomponent_get_next_property(tomb, ICAL_ANY_PROPERTY);

            switch (icalproperty_isa(prop)) {
            case ICAL_TZNAME_PROPERTY:
                icalproperty_set_tzname(prop, tombstone.name);
                break;
            case ICAL_TZOFFSETFROM_PROPERTY:
                icalproperty_set_tzoffsetfrom(prop, tombstone.offset_from);
                break;
            case ICAL_TZOFFSETTO_PROPERTY:
                icalproperty_set_tzoffsetto(prop, tombstone.offset_to);
                break;
            case ICAL_DTSTART_PROPERTY:
                /* Adjust window open to local time */
                icaltime_adjust(&start, 0, 0, 0, tombstone.offset_from);
                icaltime_set_utc(&start, 0);

                icalproperty_set_dtstart(prop, start);
                break;
            default:
                icalcomponent_remove_property(tomb, prop);
                icalproperty_free(prop);
                break;
            }
        }

        /* Remove X-PROLEPTIC-TZNAME as it no longer applies */
        if (proleptic_prop) {
            icalcomponent_remove_property(vtz, proleptic_prop);
            icalproperty_free(proleptic_prop);
        }
    }

    /* Remove any unused tombstone components */
    if (tomb_std) {
        icalcomponent_remove_component(vtz, tomb_std);
        icalcomponent_free(tomb_std);
    }
    if (tomb_day) {
        icalcomponent_remove_component(vtz, tomb_day);
        icalcomponent_free(tomb_day);
    }

    if (obsarray) {
        struct icalobservance *obs;

        /* Sort the observances by onset */
        icalarray_sort(obsarray, &observance_compare);

        /* Set offset_to for tombstone, if necessary */
        obs = icalarray_element_at(obsarray, 0);
        if (!tombstone.offset_to) tombstone.offset_to = obs->offset_from;

        /* Adjust actual range if necessary */
        if (adjust_start) {
            *startp = obs->onset;
        }
        if (adjust_end) {
            obs = icalarray_element_at(obsarray, obsarray->num_elements-1);
            *endp = obs->onset;
            icaltime_adjust(endp, 0, 0, 0, 1);
        }
    }

    if (proleptic) *proleptic = &tombstone;
}

char *read_line(char *s, size_t size, void *fp)
{
    return fgets(s, (int)size, (FILE *)fp);
}

icalcomponent *read_vtimezone(const char *path)
{
    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "read_vtimezone: can not open file %s: %s\n",
                path, strerror(errno));
        return NULL;
    }

    icalcomponent *comp = NULL;
    icalparser *parser = icalparser_new();
    icalparser_set_gen_data(parser, fp);

    char *line;
    do {
        line = icalparser_get_line(parser, read_line);
        comp = icalparser_add_line(parser, line);
    } while (line && !comp);

    fclose(fp);
    icalparser_free(parser);
    return comp;
}

struct offset {
    int32_t offset;
    uint64_t tzidx;
};

struct observance {
    int64_t onset;
    int32_t offset;
};

struct timezone {
    char *tzid;
    uint32_t obs_cnt;
    struct observance *obs;
    uint64_t idx;
};

static int compare_offset(const void *va, const void *vb)
{
    const struct offset *a = va;
    const struct offset *b = vb;
    
    if (a->offset < b->offset)
        return -1;
    else if (a->offset > b->offset)
        return 1;
    else if (a->tzidx < b->tzidx)
        return -1;
    else if (a->tzidx > b->tzidx)
        return 1;
    else
        return 0;
}

static void add_vtimezone(icalarray *timezones, icalcomponent *vtz,
                          icaltimetype start, icaltimetype end,
                          icalarray *offsets)
{
    const icaltimezone *utc = icaltimezone_get_utc_timezone();

    icalproperty *prop = icalcomponent_get_first_property(vtz, ICAL_TZID_PROPERTY);
    if (!prop) return;

    const char *tzid = icalproperty_get_tzid(prop);
    if (!tzid) return;

    /* Expand timezone observances */
    icalarray *icalobs = icalarray_new(sizeof(struct icalobservance), 20);
    icalcomponent *myvtz = icalcomponent_clone(vtz);
    truncate_vtimezone_advanced(myvtz, &start, &end, icalobs,
            NULL, NULL, NULL, NULL, 0);
    icalcomponent_free(myvtz);

    if (icalobs->num_elements) {
        struct timezone tz = { 0 };
        tz.tzid = strdup(tzid);
        tz.obs_cnt = (uint32_t) icalobs->num_elements;
        tz.obs = malloc(sizeof(struct observance) * tz.obs_cnt);

        /* Calculate timezone byte index */
        if (timezones->num_elements) {
            struct timezone *prev =
                icalarray_element_at(timezones, timezones->num_elements - 1);
            uint64_t prev_size = strlen(prev->tzid) + 1 +
                prev->obs_cnt * sizeof(struct observance);
            tz.idx = prev->idx + prev_size;
        }

        /* Determine observances and UTC offsets */
        icalarray *uniqoffsets = icalarray_new(sizeof(int32_t), 20);
        size_t i;
        for (i = 0; i < tz.obs_cnt; i++) {
            struct icalobservance *icalob = icalarray_element_at(icalobs, i);
            struct observance *ob = &tz.obs[i];
            ob->onset = (int64_t) icaltime_as_timet_with_zone(icalob->onset, utc);
            ob->offset = (int32_t) icalob->offset_to;

            /* Deduplicate UTC offsets of this timezone */
            int is_uniq = 1;
            size_t j;
            for (j = 0; j < uniqoffsets->num_elements; j++) {
                int32_t *val = icalarray_element_at(uniqoffsets, j);
                if (*val == ob->offset) {
                    is_uniq = 0;
                    break;
                }
            }
            if (is_uniq) {
                icalarray_append(uniqoffsets, &ob->offset);
            }
        }

        /* Add timezone UTC offsets to lookup table */
        for (i = 0; i < uniqoffsets->num_elements; i++) {
            int32_t *val = icalarray_element_at(uniqoffsets, i);
            struct offset off = { *val, tz.idx };
            icalarray_append(offsets, &off);
        }

        icalarray_append(timezones, &tz);
        icalarray_free(uniqoffsets);
    }

    icalarray_free(icalobs);
}

static const uint8_t version = 1;

static int write_header(icaltimetype start, icaltimetype end,
                        const char *iana_version,
                        FILE *fp)
{
    const icaltimezone *utc = icaltimezone_get_utc_timezone();
    int64_t created = (int64_t) time(NULL);
    int64_t tstart = icaltime_as_timet_with_zone(start, utc);
    int64_t tend = icaltime_as_timet_with_zone(end, utc);

    fputs("cyr_guesstz", fp);
    fputc(0, fp);
    fputc(version, fp);
    fwrite(&created, sizeof(int64_t), 1, fp);
    fwrite(&tstart, sizeof(int64_t), 1, fp);
    fwrite(&tend, sizeof(int64_t), 1, fp);
    fputs(iana_version, fp);
    fputc(0, fp);

    return 0;
}

static int write_offsets(icalarray *offsets, FILE *fp)
{
    uint64_t offsets_cnt = offsets->num_elements;

    fwrite(&offsets_cnt, sizeof(uint64_t), 1, fp);
    size_t i;
    for (i = 0; i < offsets->num_elements; i++) {
        struct offset *off = icalarray_element_at(offsets, i);
        fwrite(&off->offset, sizeof(int32_t), 1, fp);
        fwrite(&off->tzidx, sizeof(uint64_t), 1, fp);
    }

    return 0;
}

static int write_timezones(icalarray *timezones, FILE *fp)
{
    uint64_t timezones_cnt = timezones->num_elements;

    fwrite(&timezones_cnt, sizeof(uint64_t), 1, fp);
    size_t i;
    for (i = 0; i < timezones->num_elements; i++) {
        struct timezone *tz = icalarray_element_at(timezones, i);
        fputs(tz->tzid, fp);
        fputc(0, fp);
        fwrite(&tz->obs_cnt, sizeof(uint32_t), 1, fp);
        size_t j;
        for (j = 0; j < tz->obs_cnt; j++) {
            fwrite(&(tz->obs[j].onset), sizeof(int64_t), 1, fp);
            fwrite(&(tz->obs[j].offset), sizeof(int32_t), 1, fp);
        }
    }

    return 0;
}

static char *read_ianaversion(const char *zoneinfo_dir)
{
    char *ianaversion = NULL;
    char *fname = malloc(strlen(zoneinfo_dir) + 9);
    fname[0] = '\0';
    strcat(fname, zoneinfo_dir);
    strcat(fname, "/version");
    FILE *fp = fopen(fname, "r");

    if (fp) {
        char version[32];
        size_t n = fread(version, 1, 32, fp);
        if (n) {
            version[n-1] = '\0';
            ianaversion = strdup(version);
        }
    }
    if (!ianaversion) {
        ianaversion = strdup("unknown");
    }

    fclose(fp);
    free(fname);
    return ianaversion;
}

static int create_from_zonedir(const char *zoneinfo_dir,
                               icaltimetype start, icaltimetype end,
                               FILE *fp)
{
    int r = 0;

    char *paths[2] = { (char *) zoneinfo_dir, NULL };
    FTS *fts = fts_open(paths, 0, NULL);
    if (!fts) {
        fprintf(stderr, "fts_open %s: %s\n", zoneinfo_dir, strerror(errno));
        return EX_IOERR;
    }

    icalarray *timezones = icalarray_new(sizeof(struct timezone), 20);
    icalarray *offsets = icalarray_new(sizeof(struct offset), 20);

    /* Process VTIMEZONEs */
    FTSENT *fe;
    while ((fe = fts_read(fts))) {
        if (fe->fts_info != FTS_F) {
            continue;
        }
        icalcomponent *ical = read_vtimezone(fe->fts_accpath);
        if (!ical) {
            fprintf(stderr, "skipping %s\n", fe->fts_path);
            continue;
        }
        if (ical && icalcomponent_isa(ical) == ICAL_VCALENDAR_COMPONENT) {
            icalcomponent *vtz;
            for (vtz = icalcomponent_get_first_component(ical, ICAL_VTIMEZONE_COMPONENT);
                 vtz;
                 vtz = icalcomponent_get_next_component(ical, ICAL_VTIMEZONE_COMPONENT)) {

                add_vtimezone(timezones, vtz, start, end, offsets);
            }
        }
        if (ical) icalcomponent_free(ical);
    }
    fts_close(fts);
    icalarray_sort(offsets, compare_offset);

    /* Write database */
    char *ianaversion = read_ianaversion(zoneinfo_dir);
    write_header(start, end, ianaversion, fp);
    write_offsets(offsets, fp);
    write_timezones(timezones, fp);
    free(ianaversion);

    /* Free state */
    size_t i;
    for (i = 0; i < timezones->num_elements; i++) {
        struct timezone *tz = icalarray_element_at(timezones, i);
        free(tz->tzid);
        free(tz->obs);
    }
    icalarray_free(timezones);
    icalarray_free(offsets);

    return r;
}

int main(int argc, char **argv)
{
    icaltimetype start = icaltime_from_string("20000101T000000");
    icaltimetype end = icaltime_from_string("20320101T000000");

    create_from_zonedir("../zoneinfo", start, end, stdout);

}

