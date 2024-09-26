version development

workflow Shapeit5 {
  input {
    Array[File] vcfs
    Array[File] vcf_idxs
    Boolean needToSplitChromosomes
    Boolean wantFinalConcat

    Array[File] chromosome_chunks # TODO transfer to terra, convert to +scatter compatible format
    Array[File] chromosome_maps

    File whole_chromosome_scatter

    String bcftools_docker
    String shapeit5_docker = "abelean/shapeit5"
  }

  if (needToSplitChromosomes) {
    File wg_vcf = vcfs[0]
    File wg_vcf_idx = vcf_idxs[0]

    call shard_vcf as sxrm {
      input:
        vcf = wg_vcf,
        vcf_idx = wg_vcf_idx,
        scatter_regions = whole_chromosome_scatter,
        pref = sub(basename(wg_vcf), "\\.[bv]cf(\\.gz)?", "") + ".chr",
        bcftools_docker = bcftools_docker
    }

    call sort as xrm_sort {
      input:
        table = transpose([sxrm.vcf_shards, sxrm.vcf_shard_idxs]),
        sort_regex = "\\.chr_(\\d+)\\.[bv]cf(?:\\.gz)?",
    }

    Array[Array[File]] split_vcfs_info = transpose(xrm_sort.sorted_table)
  }

  Array[Array[File]] xrm_vcfs_info = select_first([split_vcfs_info, transpose([vcfs, vcf_idxs]) ])

  # Chromosome
  scatter (i in range(length(xrm_vcfs_info))) {

    call shard_vcf as schunk {
      input:
        vcf = xrm_vcfs_info[i][0]
        vcf_idx = xrm_vcfs_info[i][1]
        scatter_regions = chromosome_chunks[i]
        bcftools_docker = bcftools_docker
    }

    call sort as chunk_sort {
      input:
        table = transpose([schunk.vcf_shards, schunk.vcf_shard_idxs]),
        sort_regex = "\\.shard_(\\d+)\\.[bv]cf(?:\\.gz)?",
    }

    Array[String] chunk_regions = transpose(read_tsv(chromosome_chunks[i]))[0]

    # Chunk
    scatter (j in range(length(chunk_regions))) {
      call shapeit_phase_common {
        input:
          bcf = chunk_sort.sorted_table[j][0]
          bcf_idx = chunk_sort.sorted_table[j][1]
          chromosome_map = chromosome_maps[i]
          chunk_region = chunk_regions[j]
          docker = shapeit5_docker
      }
    }

    # Concat Chunks
  }

  # Concat Chromosomes

  output {

  }
}


task shapeit_phase_common {
  input {
    File bcf
    File bcf_idx
    File chromosome_map
    String chunk_region

    Float filter_maf = 0.001

    Int diskGB = 1800
    Boolean SSD = true
    Int memGB = 144
    Int cpu = 72
    Int preemptible = 3
    String docker = "abelean/shapeit5"
  }

  String base = sub(basename(bcf), "\\.[bv]cf(\\.gz)?^", "")
  String out = "~{base}.common_phased"

  command <<<
    SHAPEIT5_phase_common_static_v1.0.0 \
    --input ~{bcf} \
    --map ~{chromosome_map} \
    --output "~{out}.bcf" \
    --thread ~{cpu} \
    --log "~{out}.log" \
    --filter-maf ~{filter_maf} \
    --region ~{chunk_region}

    bcftools index -f "~{out}.bcf" --threads 4
  >>>

  output {
    File common_phased_bcf = "~{out}.bcf"
    File? common_phased_bcf_idx = "~{out}.bcf.csi"
    File common_phased_log = "~{out}.log"
  }

  runtime {
    disks: "local-disk ~{diskGB} {if ~{SSD} SSD else HDD}"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: docker
  }
}


task shard_vcf {
  input {
    File vcf
    File vcf_idx
    File scatter_regions
    String? pref

    String bcftools_docker
  }

  String base = sub(basename(vcf), "\\.[bv]cf(\\.gz)?", "") + ".shard"
  String out = pref if defined(pref) else base
  Int disk_gb = ceil(3 * size(vcf, "GB"))

  command <<<
    set -eu -o pipefail

    # Make an empty shard in case the input VCF is totally empty
    bcftools view -h ~{vcf} | bgzip -c > "~{out_prefix}_0.vcf.gz"

    bcftools +scatter \
      -O z3 -o . -p "~{out}_" \
      -S ~{scatter_regions} \
      ~{vcf}

    # Print all VCFs to stdout for logging purposes
    find ./ -name "*.vcf.gz"

    # Index all shards
    find ./ -name "~{out}_*.vcf.gz" \
    | xargs -I {} tabix -p vcf -f {}
  >>>

  output {
    Array[File] vcf_shards = glob("~{out}_*.vcf.gz")
    Array[File] vcf_shard_idxs = glob("~{out}_*.vcf.gz.tbi")
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "3.75 GiB"
    cpu: 4
    preemptible: 3
    maxRetries: 1
    docker: docker
  }
}


task Sort {
  # this task is *NOT* robust to inconsistent data or partially applicable regex
  #   the regex must apply consistently to every single (text) datum and produce a valid integer string

  input {
    String sort_regex
    Array[Array[String]] table
    Int on_column = 0
  }
  
  File tsv = write_tsv(table)
  
  command <<<
    python3 - <<'__script__'
    import re
    table = []
    with open('~{tsv}', 'r') as inp:
      for line in inp:
        if line == '\n': continue
        table.append(line.strip().split('\t'))
    
    sorted_table = sorted(table, key= lambda x: int(
        re.search("~{sort_regex}", x[~{on_column}] )[1]
    ))

    # avoids empty line at end
    with open('sorted.tsv', 'w') as out:
      out.write('\t'.join(sorted_table[0]))
      for line in sorted_table[1:]:
        out.write('\n' + '\t'.join(line))
    __script__
  >>>
  
  output {
    Array[Array[String]] sorted_table = read_tsv('sorted.tsv')
  }
  
  runtime {
    disks: "local-disk 10 HDD"
    memory: "4 Gi"
    cpu: 1
    preemptible: 3
    docker: "python:latest"
  }
}

