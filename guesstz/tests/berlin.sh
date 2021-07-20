#!/bin/bash
vtz="BEGIN:VCALENDAR
PRODID: -//xxx//yyy//EN
VERSION:2.0
BEGIN:VTIMEZONE
TZID:Custom
LAST-MODIFIED:20210127T134508Z
BEGIN:DAYLIGHT
TZNAME:CEST
TZOFFSETFROM:+0100
TZOFFSETTO:+0200
DTSTART:19810329T020000
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU
END:DAYLIGHT
BEGIN:STANDARD
TZNAME:CET
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
DTSTART:19961027T030000
RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU
END:STANDARD
END:VTIMEZONE
END:VCALENDAR"
trstart="20201227T140000Z"
trend="20211227T150000Z"
want="Europe/Berlin"
source "$srcdir/tests/test.sh"