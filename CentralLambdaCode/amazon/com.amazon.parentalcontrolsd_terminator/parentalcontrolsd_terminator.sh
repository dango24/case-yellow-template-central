#!/bin/bash

# This script only written for single-thread processes.

LOGGER="logger -t parentalcontrolsd_terminator -s "

APP_NAME="parentalcontrolsd"
CPU_LIMIT="5" # % of the CPU to watch for.

APP_PID=$(pgrep "$APP_NAME")

if [[ ! "$APP_PID" ]]; then
    $LOGGER "The $APP_NAME process was not found."
    exit 0
fi

APP_CPU=$(ps h -p $APP_PID -o %cpu | grep -v %CPU | sed s/' '/''/g)

CPU_PERCENTAGE=$(bc <<< $APP_CPU*100)
CPU_PERCENTAGE="${CPU_PERCENTAGE%.*}"
CPU_LIMIT_PERCENTAGE=$(bc <<< $CPU_LIMIT*100)

if [[ $CPU_PERCENTAGE -ge $CPU_LIMIT_PERCENTAGE ]]; then
    $LOGGER "$APP_NAME is using more than $CPU_LIMIT% CPU."
    $LOGGER "Attempting to shut down process nicely..."
    kill "$APP_PID"

    sleep 5 # Wait seconds to see if the process is still alive.

    if [ $(pgrep "$APP_NAME") ]; then
        $LOGGER "$APP_NAME is still running. Terminating." # Hasta la vista
        killall -9 "$APP_NAME"
    else
        $LOGGER "$APP_NAME was shut down successfully."
    fi
else
     $LOGGER "$APP_NAME is using less than $CPU_LIMIT% CPU."
fi
