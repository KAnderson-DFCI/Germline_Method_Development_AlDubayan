version development

workflow GlimpsePhaseLigate {
    input {
        String sampleid
        File bam_or_vcf

        String refid
        Array[Directory]+ refBins
        Array[String]+ chrs
    }

    scatter (i in range(length(chrs))) {
        call glimpsePhaseLigate {
            input:
              sampleid = sampleid,
              bam_or_vcf = bam_or_vcf,

              refid = refid,
              refBins = refBins[i],
              chunks = "~{refBins[i]}/chunks.txt"
        }
    }

    output {
    	File imputedVcf = glimpsePhaseLigate.imputedVcf
    }
}


task glimpsePhaseLigate {
    input {
        String sampleid
        File bam_or_vcf
        String inputFlag = '--bam-file' # or --input-gl

        String refid
        Directory refBins 
        File chunks

        Int cpus = 4
        Int preemptible = 2
    }
    
    
    String base = sep("_", [sampleid, refid])
    Int diskSpace = ceil(4 * size(vcf, "GB")) + 12
    
    command <<<
      mkdir sample_imputed

      ### PHASE ###
      # loop logic taken from the GLIMPSE2 tutorial
      while IFS="" read -r LINE || [ -n "$LINE" ]
      do
        printf -v ID "%02d" $(echo $LINE | cut -d" " -f1)
        CHR=$(echo $LINE | cut -d" " -f2)
        IRG=$(echo $LINE | cut -d" " -f3)
        REGS=$(echo $IRG | cut -d":" -f 2 | cut -d"-" -f1)
        REGE=$(echo $IRG | cut -d":" -f 2 | cut -d"-" -f2)

        GLIMPSE2_phase \
        ~{inputFlag} ~{bam_or_vcf} \
        --reference ~{refBins}/${CHR}_${CHR}_${REGS}_${REGE}.bin \ # going to fix this so first CHR is refid
        --output sample_imputed/~{base}_${CHR}_${REGS}_${REGE}.bcf \
        --threads ~{cpus}
      done < ~{chunks}
      
      ### LIGATE ###
      ls -1v sample_imputed/~{base}_*.bcf > sample_chunks_list.txt
      GLIMPSE2_ligate --input sample_chunks_list.txt --output ~{base}_imputed.bcf

    >>>

    output {
    	File imputedVcf = "~{base}_imputed.bcf"
    }

    runtime {
        disks: "local-disk ${diskSpace} HDD"
        memory: "5 GB"
        cpus: cpus
        preemptible: preemptible
        docker: "simrub/glimpse:v2.0.0-27-g0919952_20221207"
    }
}