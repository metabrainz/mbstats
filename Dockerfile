FROM metabrainz/base-image

RUN apt-get update && apt-get -y install python-influxdb

COPY files/* /tools/
