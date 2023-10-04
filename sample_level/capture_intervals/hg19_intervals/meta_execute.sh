#!/bin/bash

# author: Kyler Anderson
#
# retrieves files from meta_sources
# all currently are bigBeds from UCSC browser
#   and so converts all to beds

sources=($(cut -f 2 meta_sources.txt))
localn=(${sources[@]##*/})
beds=($(cut -f 1 meta_sources.txt))


# retrieve bigBeds
curl --remote-name-all ${sources[@]}


# convert to beds
nfiles=${#sources[@]}
i=0

while [ "$i" -lt "$nfiles" ]; do
  echo "Converting ${localn[$i]} to ${beds[$i]}_target.bed"
  bigBedtoBed "${localn[$i]}" "${beds[$i]}_target.bed"
  ((i++))
done


