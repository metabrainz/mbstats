FROM metabrainz/base-image

RUN apt-get update && apt-get -y install python-influxdb

#fix logrotate, see https://github.com/phusion/baseimage-docker/issues/338
RUN sed -i 's/^su root syslog/su root adm/' /etc/logrotate.conf

COPY files/pygtail/pygtail /tools/pygtail
COPY files/stats.parser.py /tools/
COPY files/mbstats.crontab /etc/cron.d/mbstats
