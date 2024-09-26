FROM kylera/samtools-suite:gcloud as base

RUN apt-get -qqy update --fix-missing && \
    apt-get -qqy install --fix-missing --no-upgrade --no-install-recommends \
        git

# Miniconda
RUN cd /opt/ && \
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && \
    bash miniconda.sh -b -p /opt/miniconda && \
    . /opt/miniconda/bin/activate && \
    conda init --all && \
    cd ~


# UCSC Tools
RUN mkdir /opt/ucsc && cd /opt/ucsc/ && \
    wget http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/wigToBigWig && \
      chmod +x wigToBigWig && \
    wget http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/bigWigAverageOverBed && \
      chmod +x bigWigAverageOverBed && \
    wget http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/fetchChromSizes && \
      chmod +x fetchChromSizes && \
    cd /opt/
ENV PATH=$PATH:/opt/ucsc/


##### Mosaic Forecast
RUN git clone https://github.com/parklab/MosaicForecast.git && \
    . /opt/miniconda/bin/activate && \
    conda env create --name MF --file MosaicForecast/environment.yaml 
    #&& \
    #cat /opt/miniconda/envs/MF/etc/conda/activate.d/activate-binutils_linux-64.sh && \
    #conda activate MF

#CMD ["/opt/miniconda/bin/activate"]

# Extra Files (incorporated into WDL)

# hg19
#wget ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/reference/phase2_reference_assembly_sequence/hs37d5.fa.gz
#wget ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/reference/phase2_reference_assembly_sequence/hs37d5.fa.gz.fai -O hs37d5.fa.fai

## filtered regions
#wget http://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/genomicSuperDups.txt.gz
#wget http://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/simpleRepeat.txt.gz
#~/MosaicForecast/resources/SegDup_and_clustered.GRCh37.bed

## Mappability Scores
#wget https://bismap.hoffmanlab.org/raw/hg19.umap.tar.gz  
#tar -zxvf hg19.umap.tar.gz  
#cd hg19  
#fetchChromSizes hg19 > hg19.chrom.sizes  
#wigToBigWig <(zcat k24.umap.wg.gz) hg19.chrom.sizes k24.umap.wg.bw  


# hg38
#wget ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/reference/GRCh38_reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa 
#wget ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/reference/GRCh38_reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai 

## filtered regions
#wget http://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/genomicSuperDups.txt.gz
#wget http://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/simpleRepeat.txt.gz
#~/MosaicForecast/resources/SegDup_and_clustered.GRCh38.bed

# Mappability Scores
#wget https://bismap.hoffmanlab.org/raw/hg38.umap.tar.gz  
#tar -zxvf hg38.umap.tar.gz  
#cd hg38  
#fetchChromSizes hg38 > hg38.chrom.sizes  
#wigToBigWig <(zcat k24.umap.wg.gz) hg38.chrom.sizes k24.umap.wg.bw  