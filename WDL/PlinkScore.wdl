version development

workflow PlinkScore {
    input {
    	String cohortName
        File plinkBed
        File plinkBim
        File plinkFam
        
        Array[String]+ pgsNames
        Array[File]+ pgsFiles
    }

    call plinkScore {
        input:
          base = cohortName,
          bed = plinkBed,
          bim = plinkBim,
          fam = plinkFam,
          
          pgsNames = pgsNames,
          pgsFiles = pgsFiles
    }

    output {
        String cohort = cohortName
        Directory arrays = plinkScore.arrays
        Array[File] scores = plinkScore.profiles
        Array[File] nopreds = plinkScore.nopreds
        Array[File] nosex = plinkScore.nosex
    }
}

task plinkScore {
    input {
        String base
        File bed
        File bim
        File fam
        
        Array[String]+ pgsNames
        Array[File]+ pgsFiles

        Int memory = 256
        Int cpus = 8
        Int preemptible = 0
    }
    
    Int diskSpace = ceil(1.5 * size(bed, "Gi"))
    Int plinkMem = ceil(memory * 1024 * 0.9)
    
    command <<<
        # Identify Duplicate Ids
        echo "Identifying Duplicate Variant Ids"
        cut -f 2 ~{base}.bim | sort | uniq -d > duplicate_ids.txt
        
        # Perform PRS calculation
        echo "Performing PRS Calculation"
        
        names=(~{sep=" " pgsNames})
        files=(~{sep=" " pgsFiles})
        n=${#names[@]}
        
        mkdir arrays
        
        for i in $(seq 0 $((n-1)))
        do
          outn=~{base}.${names[$i]}
          plink \
          --bfile ~{base} \
          --score ${files[$i]} header no-mean-imputation \
          --exclude duplicate_ids.txt \
          --out $outn \
          --memory ~{plinkMem}
          
          for ext in profile nopred nosex
          do
            mv $outn.$ext arrays
            echo arrays/$outn.$ext >> ${ext}_list.txt
          done
        done
    >>>

    output {
        Directory arrays = "arrays"
        Array[File] profiles = read_lines("profile_list.txt")
        Array[File] nopreds = read_lines("nopred_list.txt")
        Array[File] nosex = read_lines("nosex_list.txt")
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory} GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/plink:1.90b6.21--h779adbc_1"
    }

}