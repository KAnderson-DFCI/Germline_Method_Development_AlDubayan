FROM ubuntu:focal-20220531
MAINTAINER "Kyler Anderson <kyler_anderson@dfci.harvard.edu>"

# Install basic linux libraries and dev packages
RUN apt-get clean && \
    apt-get -qqy update && \
    apt-get -qqy install --fix-missing \
      apt-utils \
      build-essential \
      zlib1g-dev \
      gzip \
      less \
      default-libmysqlclient-dev \
      curl \
      wget \
      file

ENV DEBIAN_FRONTEND=noninteractive
ENV DEBIAN_PRIORITY=critical
RUN apt-get -qqy update && \
    apt-get -qqy install --fix-missing \
      libncurses5-dev \
      libbz2-dev \
      liblzma-dev \
      libcurl4-openssl-dev \
      libssl-dev

# Install the suite
ARG SUITE_VERSION=1.18
RUN cd /opt/ && \
    wget https://github.com/samtools/htslib/releases/download/${SUITE_VERSION}/htslib-${SUITE_VERSION}.tar.bz2 && \
    tar xjf htslib-${SUITE_VERSION}.tar.bz2 && \
    cd htslib-${SUITE_VERSION} && \
    ./configure && \
    make && \
    make install && \
    cd -

RUN cd /opt/ && \
    wget https://github.com/samtools/samtools/releases/download/${SUITE_VERSION}/samtools-${SUITE_VERSION}.tar.bz2 && \
    tar xjf samtools-${SUITE_VERSION}.tar.bz2 && \
    cd samtools-${SUITE_VERSION} && \
    ./configure && \
    make && \
    make install && \
    cd -

RUN cd /opt/ && \
    wget https://github.com/samtools/bcftools/releases/download/${SUITE_VERSION}/bcftools-${SUITE_VERSION}.tar.bz2 && \
    tar xjf bcftools-${SUITE_VERSION}.tar.bz2 && \
    cd bcftools-${SUITE_VERSION} && \
    ./configure && \
    make && \
    make install && \
    cd -

# Entrypoint
CMD ["/bin/bash"]