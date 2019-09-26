#!/bin/bash

start_mbstats() {
	# full name of the file log
    local FILE=$1

	# full path to directory containing the log file
    local LOGDIR=/var/docker-logs/openresty/var/log/nginx/

    local NAME=$(FILE%%.stats.log}
    local CONTAINER_NAME=mbstats-$NAME
    local VOLUME_NAME=$CONTAINER_NAME

    docker pull metabrainz/mbstats
    docker volume create --driver local --name $VOLUME_NAME

    docker run \
        --detach \
        --restart unless-stopped \
        --name $CONTAINER_NAME \
        --hostname $HOSTNAME \
        --volume $LOGDIR:/logs \
        --volume $VOLUME_NAME:/data \
        -e "file=/logs/$FILE" \
        -e "influx_host=stats.metabrainz.org" \
        -e "debug=true" \
        metabrainz/mbstats /sbin/my_init
}

mbstats "somefile.stats.log"
