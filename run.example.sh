#!/bin/bash

LOGDIR=/var/log/
FILE=mywebsite.stats.log

NAME=$(echo -n $FILE |sed 's/\.stats.log$//')
CONTAINER_NAME=mbstats-($NAME)
VOLUME_NAME=$CONTAINER_NAME

docker volume create --driver local --name $VOLUME_NAME

docker run \
	--detach \
	--restart unless-stopped \
	--name $CONTAINER_NAME \
	--hostname $(hostname -s) \
	--volume $LOGDIR:/logs \
	--volume $VOLUME_NAME:/data \
	-e "file=/logs/$FILE" \
	-e "influx_host=my.influx.host.example.org" \
	--net=host \
	mbstats /sbin/my_init
