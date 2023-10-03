#!/bin/bash

brands=($(cut -f 1 ../hg19_intervals/meta_sources.tsv))


for i in ${brands[@]}; do
  echo Converting $1

  base="${i}_target"
  liftOver "../hg19_intervals/${base}.bed" "../hg19ToHg38.over.chain" "${base}.bed" "../hg19_intervals/${base}_unlifted.bed"
  bedtools sort -g "../hg38.txt" -i "${base}.bed" > "${base}_sorted.bed"
done
