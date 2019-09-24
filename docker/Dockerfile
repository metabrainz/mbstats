FROM metabrainz/base-image
ENV DEBIAN_FRONTEND noninteractive

#fix logrotate, see https://github.com/phusion/baseimage-docker/issues/338
RUN sed -i 's/^su root syslog/su root adm/' /etc/logrotate.conf

COPY docker/mbstats.crontab /etc/cron.d/mbstats
RUN chmod 644 /etc/cron.d/mbstats

# for some reason this package install causes the build to fail
RUN apt-mark hold util-linux

RUN apt-get update \
  && apt-get install --reinstall -y --no-install-recommends python3 python3-virtualenv \
  && apt -y upgrade \
  && apt -y autoremove \
  && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m virtualenv --python=/usr/bin/python3 $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

# Run the application:
COPY mbstats/ /mbstats/
