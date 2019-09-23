FROM metabrainz/base-image

#fix logrotate, see https://github.com/phusion/baseimage-docker/issues/338
RUN sed -i 's/^su root syslog/su root adm/' /etc/logrotate.conf

COPY docker/mbstats.crontab /etc/cron.d/mbstats

RUN apt-get update && apt-get install \
  -y --no-install-recommends python3 python3-virtualenv

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m virtualenv --python=/usr/bin/python3 $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

# Run the application:
COPY mbstats/ /mbstats/
