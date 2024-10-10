version development

import "https://raw.githubusercontent.com/vanallenlab/pancan_germline_wgs/wes_addenda/wdl/Utilities.wdl" as Util

workflow Shapeit5 {
  input {
    String cohortName

    Array[File] vcfs
    Array[File] vcf_idxs
    Boolean needToSplitChromosomes
    Boolean wantConcatChromosomes

    File chromosome_scatter
    Array[File] chromosome_chunks_L
    Array[File] chromosome_chunks_S
    Array[File] chromosome_maps

    String bcftools_docker = "kylera/samtools-suite:gcloud"
    String shapeit5_docker = "abelean/shapeit5"
  }

  if (needToSplitChromosomes) {
    File wg_vcf = vcfs[0]
    File wg_vcf_idx = vcf_idxs[0]

    call Util.ShardVcfByRegion as sxrm {
      input:
        vcf = wg_vcf,
        vcf_idx = wg_vcf_idx,
        scatter_regions = chromosome_scatter,
        sterm = ".chr",
        bcftools_docker = bcftools_docker
    }

    call Util.SortOrder as sxrm_sort {
      input:
        items = sxrm.vcf_shards,
        sort_regex = "\\.chr(\\d+)\\.[bv]cf(?:\\.gz)?"
    }
  }

  Array[File] xrm_vcfs = select_first([sxrm.vcf_shards, vcfs])
  Array[File] xrm_vcf_idxs = select_first([sxrm.vcf_shard_idxs, vcf_idxs])

  Array[Int] N = range(length(xrm_vcfs))
  Array[Int] I = select_first([sxrm_sort.subindex, N])

  # Chromosome Scatter
  scatter (i in N) {
	Int Ii = I[i] # sorry about this, wdl doesn't like nested array indices
    
    # --------------------------------
    ### Phase Common Variants
    call Util.ShardVcfByRegion as chunkL {
      input:
        vcf = xrm_vcfs[Ii],
        vcf_idx = xrm_vcf_idxs[Ii],
        scatter_regions = chromosome_chunks_L[i],
        sterm = ".shard_",
        bcftools_docker = bcftools_docker
    }

    call Util.SortOrder as chunkL_sort {
      input:
        items = chunkL.vcf_shards,
        sort_regex = "\\.shard_(\\d+)\\.[bv]cf(?:\\.gz)?"
    }

    Array[String] chunkL_regions = transpose(read_tsv(chromosome_chunks_L[i]))[0]
    Array[Int] J = chunkL_sort.subindex

    scatter (j in range(length(J))) {
      Int Jj = J[j]
      
      call shapeit_phase_common {
        input:
          bcf = chunkL.vcf_shards[Jj],
          bcf_idx = chunkL.vcf_shard_idxs[Jj],
          chromosome_map = chromosome_maps[i],
          chunk_region = chunkL_regions[j],
          docker = shapeit5_docker
      }
    }

    # --------------------------------
    ### Ligate Into Scaffold
    call shapeit_ligate {
      input:
        bcfs = shapeit_phase_common.common_phased_bcf,
        pref = "~{cohortName}.common_phased.chr~{i}"
    }

    # --------------------------------
    ### Phase Rare Variants
    Array[Array[String]] chunkS_info = transpose(read_tsv(chromosome_chunks_S[i]))
    File ccs = write_tsv(transpose([chunkS_info[1], chunkS_info[0]]))

    call Util.ShardVcfByRegion as chunkS {
      input:
        vcf = shapeit_ligate.ligated_bcf,
        vcf_idx = shapeit_ligate.ligated_bcf_idx,
        scatter_regions = ccs,
        sterm = ".shard_",
        bcftools_docker = bcftools_docker
    }

    call Util.SortOrder as chunkS_sort {
      input:
        items = chunkS.vcf_shards,
        sort_regex = "\\.shard_(\\d+)\\.[bv]cf(?:\\.gz)?"
    }

    Array[Int] K = schunkS_sort.subindex

    scatter (k in range(length(K))) {
      Int Kk = K[k]
      
      call shapeit_phase_rare {
        input:
          bcf_unphased = xrm_vcfs[Ii],
          bcf_unphased_idx = xrm_vcf_idxs[Ii],
          bcf_commonphased = chunkS.vcf_shards[Kk],
          bcf_commonphased_idx = chunkS.vcf_shard_idxs[Kk],
          chromosome_map = chromosome_maps[i],
          chunk_region = chunkS_info[1][k],
          scaffold_region = chunkS_info[2][k],
          docker = shapeit5_docker
      }
    }
    
    # --------------------------------
    ### Concat Fully Phased Chunks
    call Util.ConcatVcfs as concat_chunkS {
      input:
        vcfs = shapeit_phase_rare.rare_phased_bcf,
        vcf_idxs = shapeit_phase_rare.rare_phased,
        out_prefix = "~{cohortName}.hap_phased.chr~{i}",
        bcftools_concat_options = "-n"
    }
  }

  # Concat Chromosomes
  if (wantConcatChromosomes) {
    call Util.ConcatVcfs as concat_xrms {
      input:
        vcfs = concat_chunkS.merged_vcf,
        vcf_idxs = concat_chunks.merged_vcf_idx,
        out_prefix = "~{cohortName}.hap_phased",
        bcftools_concat_options = "-n"
    }
  }

  output {
    Array[File] phased_chromosome_vcfs = concat_chunkS.merged_vcf
    Array[File] phased_chromosome_vcf_idxs = concat_chunkS.merged_vcf_idx
    File? phased_vcf = concat_xrms.merged_vcf
    File? phased_vcf_idx = concat_xrms.merged_vcf_idx
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

  String here_bcf = basename(bcf)
  String base = sub(here_bcf, "\\.[bv]cf(\\.gz)?", "")
  String out = "~{base}.common_phased"

  command <<<
    set -eu -o pipefail
  
    ln -s ~{bcf} ~{here_bcf}
    ln -s ~{bcf_idx} ~{basename(bcf_idx)}
  
    phase_common_static \
    --input ~{here_bcf} \
    --region ~{chunk_region} \
    --map ~{chromosome_map} \
    --filter-maf ~{filter_maf} \
    --output "~{out}.bcf" \
    --log "~{out}.log" \
    --thread ~{cpu}

    # bcftools index -f "~{out}.bcf" --threads 6
  >>>

  output {
    File common_phased_bcf = "~{out}.bcf"
    #File common_phased_bcf_idx = "~{out}.bcf.csi"
    File common_phased_log = "~{out}.log"
  }

  runtime {
    disks: "local-disk ~{diskGB} ~{if SSD then "SSD" else "HDD"}"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: docker
  }
}


task shapeit_ligate {
  input {
    File bcfs
    String pref

    Int diskGB = 200
    Boolean SSD = true
    Int memGB = 16
    Int cpu = 8
    Int preemptible = 3
    String docker = "abelean/shapeit5"
  }
  
  command <<<
    set -eu -o pipefail
  
    ligate_static \
    --input ~{write_lines(bcf_list)} \
    --output "~{pref}.bcf" \
    --thread ~{cpu}

    bcftools index -f "~{pref}.bcf" --threads ~{cpu}
  >>>

  output {
    File ligated_bcf = "~{pref}.bcf"
    File? ligated_bcf_idx = "~{pref}.bcf.csi"
  }

  runtime {
    disks: "local-disk ~{diskGB} ~{if SSD then "SSD" else "HDD"}"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: docker
  }
}


task shapeit_phase_rare {
  input {
    File bcf_unphased
    File bcf_unphased_idx
    File bcf_commonphased
    File bcf_commonphased_idx

    File chromosome_map
    String chunk_region
    String scaffold_region

    Float filter_maf = 0.001

    Int diskGB = 1800
    Boolean SSD = true
    Int memGB = 256
    Int cpu = 32
    Int preemptible = 3
    String docker = "abelean/shapeit5"
  }

  String here_bcf_un = basename(bcf_unphased)
  String here_bcf_common = basename(bcf_commonphased)
  String base = sub(here_bcf_common, "\\.[bv]cf(\\.gz)?", "")
  String out = "~{base}.rare_phased"

  command <<<
    set -eu -o pipefail
  
    ln -s ~{bcf_unphased} ~{here_bcf_un}
    ln -s ~{bcf_idx} ~{basename(bcf_idx)}
  
    phase_rare_static \
     --input-plain ~{here_bcf_un} \
     --scaffold ~{bcf_commonphased} \
     --input-region ~{chunk_region} \ # column 4 from yada yada, for sharding as well
     --scaffold-region ~{scaffold_region} \ # column 3
     --map ~{chromosome_map} \
     --output "~{out}.bcf" \
     --log "~{out}.log" \
     --thread ${cpu}
    
    # bcftools index -f "~{out}.bcf" --threads 6
  >>>

  output {
    File rare_phased_bcf = "~{out}.bcf"
    #File rare_phased_bcf_idx = "~{out}.bcf.csi"
    File rare_phased_log = "~{out}.log"
  }

  runtime {
    disks: "local-disk ~{diskGB} ~{if SSD then "SSD" else "HDD"}"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: docker
  }
}