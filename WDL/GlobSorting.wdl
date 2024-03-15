version 1.0

import "https://raw.githubusercontent.com/vanallenlab/pancan_germline_wgs/wes_addenda/wdl/Utilities.wdl" as Tasks

workflow Vep {
  input {
    Array[File] vcfs
    Array[File] vcf_idxs

    Boolean shard_vcfs = true
    Int records_per_shard = 50000
    Int max_buffered_span = 1000000
    Int remote_query_buffer = 2
    #Boolean combine_output_vcfs = false
    String? cohort_prefix

    String bcftools_docker
  }

  scatter ( vcf_info in zip(vcfs, vcf_idxs) ) {
    File vcf = vcf_info.left
    File vcf_idx = vcf_info.right

    if (shard_vcfs) {
      call Tasks.ShardVcf {
        input:
          vcf = vcf,
          vcf_idx = vcf_idx,
          records_per_shard = records_per_shard,
          bcftools_docker = bcftools_docker
      }

      call Sort as sort_large{
        input:
          sort_regex = ".*\\.(\\d{1,})\\.vcf\\.gz" # the previous glob: {out_prefix}.*.vcf.gz
          table = transpose(ShardVcf.vcf_shards, ShardVcf.vcf_shard_idxs) # column 0 is vcf, column 1 is idx
      }

      scatter ( shard_info in sort_large.sorted_table ) {
        call Tasks.SplitRegions {
          input:
            vcf = shard_info[0],
            region_span = max_buffered_span,
            variant_buffer = remote_query_buffer
        }

        call Tasks.ShardVcfByRegion {
          input:
            vcf = shard_info[0],
            vcf_idx = shard_info[1],
            scatter_regions = SplitRegions.regions,
            bcftools_docker = bcftools_docker
        }

        call Sort as sort_fine {
          input:
            sort_regex = ".*\\.(\\d{1,})\\.vcf\\.gz" # the previous glob: {out_prefix}.*.vcf.gz
                            # this also inherits from the previous name, so its important
                            #   the file extensions are handled properly by the shard task
            table = transpose(ShardVcfByRegion.vcf_shards, ShardVcfByRegion.vcf_shard_idxs)
        }
      }

      Array[File] vcf_fine_shards = flatten(ShardVcfByRegion.vcf_shards)
      Array[File] vcf_fine_shard_idxs = flatten(ShardVcfByRegion.vcf_shard_idxs)
    }
    
    Array[File] vcf_shards = select_first([vcf_fine_shards, [vcf]])
    Array[File] vcf_shard_idxs = select_first([vcf_fine_shard_idxs, [vcf_idx]])
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
  
  Int memGb = ceil(2.5 * size(tsv, 'Gi'))
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
        re.search('~{index_re}', x[~{on_column}] )[1]
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