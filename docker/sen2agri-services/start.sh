#!/bin/sh
if [ ! -e /var/lib/dbus/machine-id ]; then
    dbus-uuidgen > /var/lib/dbus/machine-id
fi

mkdir /var/run/dbus
dbus-daemon --config-file=/usr/share/dbus-1/system.conf

munged --force &
sleep 1
slurmdbd
slurmctld
slurmd

su -c sen2agri-executor sen2agri-service &
su -c sen2agri-orchestrator sen2agri-service &
