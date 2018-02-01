#!/usr/bin/env bash

COOKIE=$(python3.4 get_cookie.py --url https://megadev.labs.stratio.com --username admin --password 1234)
echo $COOKIE

jobid=$1

curl -X POST -k --cookie "dcos-acs-auth-cookie=$COOKIE" \
https://megadev.labs.stratio.com/service/spark-fw/v1/submissions/kill/$jobid
