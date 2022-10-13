#!/bin/bash
redis-cli flushall
rm -r stats/*
go build
timestamp=$(date +%s)
exec 3>&1 4>&2
trap 'exec 2>&4 1>&3' 0 1 2 3
exec 1>'stats/sps-storm-'"$timestamp"'.log' 2>&1
./sps-storm