FROM kylera/samtools-suite:latest as base

RUN apt-get -qqy update --fix-missing && \
    apt-get -qqy install --fix-missing --no-upgrade --no-install-recommends \
      git

RUN cd /opt/ && \
    git clone https://github.com/atks/vt.git && \
    cd vt && \
    git submodule update --init --recursive && \
    make && \
    cd -