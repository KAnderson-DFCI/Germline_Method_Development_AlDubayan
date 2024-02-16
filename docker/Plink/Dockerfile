FROM kylera/samtools-suite:gcloud as base

RUN apt-get -qqy update --fix-missing && \
    apt-get -qqy install --fix-missing --no-upgrade --no-install-recommends \
      git liblapack-dev libatlas-base-dev

RUN cd /opt/ && \
    wget https://github.com/chrchang/plink-ng/archive/refs/tags/v2.00a5.10.tar.gz && \
    tar xzf v2.00a5.10.tar.gz && \
    cd plink-ng-2.00a5.10/2.0/build_dynamic && \
    make && \
    cd -

# Entrypoint
CMD ["/bin/bash"]