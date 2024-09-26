version development

import "https://raw.githubusercontent.com/vanallenlab/pancan_germline_wgs/wes_addenda/wdl/Utilities.wdl" as Tasks

workflow {
  input {
    String cohortName
    Array[File] vcfs
    Array[File] vcf_idxs

    File scatter_regions_bed
    File full_target_bed

    bcftools_docker = 
  }

  scatter ( vcf_i in zip(vcfs, vcf_idxs) ) {
    call Tasks.ShardVcfByRegion as SplitVcfs {
      input:
        vcf = vcf_i.left,
        vcf_idx = vcf_i.right,
        scatter_regions = scatter_regions_bed,
        bcftools_docker = bcftools_docker
    }
  }

  Array[Array[File]] vcf_shards_by_sample = SplitVcfs.vcf_shards
  
  call Sort {
    input:
      sort_regex = ".*\\.sharded\\.(\\d+)\\.vcf\\.gz",
      table = transpose(vcf_shards_by_sample)
  }

  Array[Array[File]] vcf_shards_by_region = Sort.sorted_table

  scatter ( k in range(length(vcf_shards_by_region)) ) {
    call glnexus {
      input:
        vcfs = vcf_shards_by_region[k],
        cohortName = "~{cohortName}.region~{k}",
        targetRegions = full_target_bed
    }
  }

  call Tasks.ConcatVcfs as concat {
    input:
      vcfs = glnexus.pvcf,
      vcf_idxs = glnexus.pvcf_idx,
      out_prefix = cohortName,
      bcftools_concat_options = "-n",
      bcftools_docker = bcftools_docker
  }

  output {
    File full_pvcf = concat.merged_vcf
    File full_pvcf_idx = concat.merged_vcf_idx
    Array[File] shard_resources = glnexus.resource_log
  }

}


task glnexus {
    input {
        String cohortName
        Array[File] vcfs
        File targetRegions
        
        String glnexus_config = "DeepVariantWES"
        String outFormat = "z"
        
        Int cpu = 16
        Int memory = 64
        Int diskSize = 1024
        Int preemptible = 3
    }
    
    Int tm2 = cpu - 6
    
    String suf = if outFormat == "v" then ".vcf" else (
                 if outFormat == "z" then ".vcf.gz" else (
                 if outFormat == "u" then ".bcf" else (
                 if outFormat == "b" then ".bcf.gz" else ".vcf")))
    
    command <<<
        function runtimeInfo() {
          echo [$(date)]
          echo \* CPU usage: $(top -bn 2 -d 0.01 | grep '^%Cpu' | tail -n 1 | awk '{print $2}')%
          echo \* Memory usage: $(free -m | grep Mem | awk '{ OFMT="%.0f"; print ($3/$2)*100; }')%
          echo \* Disk usage: $(df | grep cromwell_root | awk '{ print $5 }')
        }
        while true;
          do
          runtimeInfo >> resource.log;
          runtimeInfo;
          sleep 5;
        done &
    
        glnexus_cli \
          --config ~{glnexus_config} \
          --bed ~{targetRegions} \
          --threads ~{tm2} \
          --list ~{write_lines(vcfs)} \
        | bcftools view \
          -O ~{outFormat} -o ~{cohortName}.p~{suf} \
          --threads 4 \
          -
          
        bcftools index ~{cohortName}.p~{suf} --threads 4
    >>>
    
    output {
        File pvcf = "~{cohortName}.p~{suf}"
        File? pvcf_idx = "~{cohortName}.p~{suf}.csi"
        File resource_log = "resource.log"
    }
    
    runtime {
        disks: "local-disk ~{diskSize} SSD"
        cpu: cpu
        memory: memory + "GB"
        preemptible: preemptible
        docker: "ghcr.io/dnanexus-rnd/glnexus:v1.4.1"
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
  
  Int memGB = ceil(2.5 * size(tsv, 'Gi'))
  Int diskSize = memGB
  
  command <<<
    python3 - <<'__script__'
    import re
    table = []
    with open('~{tsv}', 'r') as inp:
      for line in inp:
        if not (w := line.strip()): continue # yay walrus
        table.append(w.split('\t'))
    
    sorted_table = sorted(table, key=
      lambda x: int(
        re.search('~{sort_regex}', x[~{on_column}] )[1]
      )
    )

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
    disks: "local-disk ~{diskSize} HDD"
    memory: "~{memGB} Gi"
    cpu: 1
    preemptible: 3
    docker: "python:latest"
  }
}