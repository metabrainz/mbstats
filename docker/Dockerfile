FROM metabrainz/base-image
ENV DEBIAN_FRONTEND noninteractive

#fix logrotate, see https://github.com/phusion/baseimage-docker/issues/338
RUN sed -i 's/^su root syslog/su root adm/' /etc/logrotate.conf

COPY docker/mbstats.crontab /etc/cron.d/mbstats
RUN chmod 644 /etc/cron.d/mbstats

# for some reason this package install causes the build to fail
RUN apt-mark hold util-linux

RUN apt-get update -y \
  && apt -y upgrade \
  && apt-get install -y software-properties-common git \
  && apt -y autoremove \
  && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository ppa:deadsnakes/ppa && \
        apt-get update -y  && \
        apt-get install -y python3.6 python3-pip python3.6-venv && \
        python3.6 -m pip install pip --upgrade && \
        python3.6 -m pip install wheel && \
	apt -y autoremove && \
	rm -rf /var/lib/apt/lists/*


ENV VIRTUAL_ENV=/opt/venv
RUN python3.6 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies:
RUN bash -c "source $VIRTUAL_ENV/bin/activate"
RUN pip install --upgrade pip

COPY mbstats/ /opt/mbstats/mbstats/
COPY requirements.txt Makefile setup.py /opt/mbstats/
WORKDIR /opt/mbstats/
RUN find . -type f -name "*.py[co]" -delete
RUN find . -type d -name "__pycache__" -delete
RUN pip install -r requirements.txt && make test
RUN find . -type f -name "*.py[co]" -delete
RUN find . -type d -name "__pycache__" -delete
RUN ls -lR /opt/mbstats
