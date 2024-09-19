# The Germline Genomics of Cancer (G2C)
# Copyright (c) 2023-Present, Ryan L. Collins and the Dana-Farber Cancer Institute
# Contact: Ryan Collins <Ryan_Collins@dfci.harvard.edu>
# Distributed under the terms of the GNU GPL v2.0

FROM ubuntu:focal-20230301
MAINTAINER "Ryan Collins <Ryan_Collins@dfci.harvard.edu>"

# Install wget
RUN apt-get update && apt-get install -y wget && apt-get clean

# Download sra-toolkit
ARG VERSION=3.1.0
RUN cd /opt && \
    wget http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/${VERSION}/sratoolkit.${VERSION}-ubuntu64.tar.gz && \
    tar -xzf sratoolkit.${VERSION}-ubuntu64.tar.gz && \
    rm -r sratoolkit.${VERSION}-ubuntu64.tar.gz && \
    cd -

# Configure sra-toolkit
RUN mkdir -p /root/.ncbi
COPY user-settings.mkfg /root/.ncbi/user-settings.mkfg
ENV PATH="$PATH:/opt/sratoolkit.${VERSION}-ubuntu64/bin"

# Install samtools dependencies
RUN apt-get update && \
    apt-get install -y gcc make zlib1g-dev libbz2-dev liblzma-dev \
                       libcurl4-openssl-dev libssl-dev libncurses5-dev && \
    apt-get clean

# Install samtools
ARG SAMTOOLS_VERISON=1.18
RUN wget https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERISON}/samtools-${SAMTOOLS_VERISON}.tar.bz2 && \
    tar -xvf samtools-${SAMTOOLS_VERISON}.tar.bz2 && \
    cd samtools-${SAMTOOLS_VERISON} && \
    ./configure --without-curses && \
    make && \
    make install && \
    cd -

# Developer aliases, etc (for convenience)
RUN echo 'alias l="ls -ltrha"' >> ~/.bashrc && \
    echo 'alias less="zless"' >> ~/.bashrc

# Launch bash as entrypoint
CMD ["/bin/bash"]
