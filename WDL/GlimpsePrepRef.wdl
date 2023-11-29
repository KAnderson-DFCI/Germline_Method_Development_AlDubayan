version development

workflow GlimpsePrepRef {
    input {
        String refid
        Array[File]+ vcfs
        Array[File]+ vcf_indices
        Array[String]+ chrs
        
        String? sort_regex
    }
    
    if (defined(sort_regex)) {
    	String index_re = select_first([sort_regex])
    
    	call sort {
        	input:
              index_re = index_re,
              table = transpose([vcfs, vcf_indices])
        }
        
        Array[Array[String]] paths = transpose(sort.sorted_table)
        Array[File] s_vcfs = paths[0]
        Array[File] s_indices = paths[1]
    }
    
    Array[File] sorted_vcfs = select_first([s_vcfs, vcfs])
    Array[File] sorted_vcf_indices = select_first([s_indices, vcf_indices])

    scatter (i in range(length(vcfs))) {
        call glimpse_chunk_split {
            input:
              vcf = sorted_vcfs[i],
              index = sorted_vcf_indices[i],
              region = chrs[i],
              prefix = refid
        }
    }

    output {
    	Array[File] chunkLists = glimpse_chunk_split.chunk_list
        Array[Directory] chunkDirs = glimpse_chunk_split.region_folder
    }
}

task sort {
    input {
      String index_re
      Array[Array[String]] table
    }
    
    File tsv = write_tsv(table)
    
    Int diskSize = ceil(2.1 * size(tsv, 'Gi'))
    
    command <<<
      python3 - <<'__script__'
      import re
      table = []
      with open('~{tsv}','r') as inp:
        for line in inp:
          if not line.strip(): continue
          table.append(line.strip().split('\t'))
      
      sorted_table = sorted(table, key=lambda x: int(re.search('~{index_re}', x[0])[1]))
      tsv_text = '\n'.join(['\t'.join(line) for line in sorted_table])
      
      with open('sorted.tsv','w') as out:
        out.write(tsv_text)
      __script__
    >>>
    
    output {
      Array[Array[String]] sorted_table = read_tsv('sorted.tsv')
    }
    
    runtime {
      disks: "local-disk ~{diskSize} HDD"
      memory: "1 Gi"
      cpu: 1
      preemptible: 3
      docker: "python:latest"
    }
}

task glimpse_chunk_split {
    input {
        File vcf
        File index
        String region
        String prefix
		
        Int cpus = 4
        Int preemptible = 3
    }
    
    
    String base = sep("_", [prefix, region, "glimpse-bins"])
    Int diskSpace = ceil(1.5 * size(vcf, "GB"))
	
    String vcfHere = basename(vcf)
    String indexHere = basename(index)
    
    command <<<
      mkdir ~{base}
      cd ~{base}
      
      # these may have been separated by past workflows (inappropriately)
      #   so bring them back together here
      mv ~{vcf} ~{vcfHere}
      mv ~{index} ~{indexHere}
    
      GLIMPSE2_chunk \
      --input ~{vcfHere} \
      --region ~{region} \
      --sequential \
      --output chunks.txt

      # loop logic taken from the GLIMPSE2 tutorial
      while IFS="" read -r LINE || [ -n "$LINE" ]
      do
        printf -v ID "%02d" $(echo $LINE | cut -d" " -f1)
        IRG=$(echo $LINE | cut -d" " -f3)
        ORG=$(echo $LINE | cut -d" " -f4)

        GLIMPSE2_split_reference \
        --reference ~{vcfHere} \
        --input-region $IRG \
        --output-region $ORG \
        --output ~{prefix}
      done < chunks.txt
      
      rm ~{vcfHere}
      rm ~{indexHere}
      cd ..
    >>>

    output {
    	File chunk_list = "~{base}/chunks.txt"
        Directory region_folder = "~{base}"
    }

    runtime {
        disks: "local-disk ${diskSpace} HDD"
        memory: "5 GB"
        cpus: cpus
        preemptible: preemptible
        docker: "simrub/glimpse:v2.0.0-27-g0919952_20221207"
    }
}