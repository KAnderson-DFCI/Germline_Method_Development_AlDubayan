FROM kylera/samtools-suite:latest as base

RUN apt-get -qqy update --fix-missing && \
    apt-get -qqy install --fix-missing --no-upgrade --no-install-recommends \
      git \
      libexpat1-dev

# Dependencies: tclap, Rmath, pcre2, libsvm
RUN cd /opt/ && \
    wget https://sourceforge.net/projects/tclap/files/tclap-1.2.2.tar.gz && \
    tar xzf tclap-1.2.2.tar.gz && \
    cd tclap-1.2.2 && \
    ./configure && \
    make && \
    make install && \
    cd -

RUN cd /opt/ && \
    git clone https://github.com/atks/Rmath.git && \
    cd Rmath && \
    make && \
    make install && \
    cd -

RUN cd /opt/ && \
    wget https://sourceforge.net/projects/pcre/files/pcre2/10.31/pcre2-10.31.tar.gz && \
    tar xzf pcre2-10.31.tar.gz && \
    cd pcre2-10.31 && \
    ./configure && \
    make && \
    make install && \
    cd -

RUN cd /opt/ && \
    git clone https://github.com/cjlin1/libsvm.git && \
    cd libsvm && \
    make && \
    make install && \
    cd -

RUN cd /opt/ && \
    git clone https://github.com/atks/vt.git && \
    cd vt && \
    git submodule update --init --recursive && \
    make && \
    make install && \
    cd -