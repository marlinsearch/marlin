FROM ubuntu:18.04


MAINTAINER MarlinSearch <searchmarlin@gmail.com>

RUN DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::ForceIPv4=true update && apt-get -y install autoconf libtool git ssh tar gzip ca-certificates && DEBIAN_FRONTEND=noninteractive apt-get -y install cmake build-essential gcc zlib1g-dev libssl-dev lcov python python-requests psmisc tzdata curl python-pip && pip install --upgrade RESTinstance && pip install robotframework && rm -rf /var/lib/apt/lists/*
