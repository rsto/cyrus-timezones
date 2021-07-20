#!/bin/bash
guesstzdb="$srcdir/../zoneinfo/guesstz.db"
val=`echo "$vtz" | sed 's/$/\r/g'  | ./cyr_guesstz -r $trstart/$trend $guesstzdb`
if [ "$val" = "$want" ]; then
    exit 0
else
    exit -1
fi
