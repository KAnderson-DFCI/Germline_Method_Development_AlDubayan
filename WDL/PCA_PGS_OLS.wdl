workflow PCA_PGS_OLS {
    input {
        String setID
    	Array[String]+ sampleIDs
        Array[File]+ sampleVcfs
        
        String refID
        File ref_plinkBed
        File ref_plinkBim
        File ref_plinkFam

        Array[String]+ pgsNames
        Array[File]+ pgsFiles
    }


    if (length(SampleIDs) > 1) {
        call bcfMerge {
            input:
                setID = setID,
                vcfs = sampleVcfs
        }
    }

    if (length(sampleIDs) == 1) {
        String name1 = sampleIDs[0]
    }

    String caseName = select_first([name1, setID])
    File caseVcf = select_first([bcfMerge.mergedVcf, sampleVcfs[0]])
    String cohortName = "~{caseName}_~{refID}"

    call plinkMerge as full {
        input:
            base = cohortName,
            newVcfs = caseVcf,
            oldBed = ref_plinkBed,
            oldBim = ref_plinkBim,
            oldFam = ref_plinkFam
    }

    call plinkPCA {
        input:
          base = cohortName,
          binaries = full.binaries
    }

    call plinkScore {
        input:
          base = cohortName,
          binaries = full.binaries,
          
          pgsNames = pgsNames,
          pgsFiles = pgsFiles
    }

    #call olsResidualize {
    #    input:
    #        pca = plinkPCA.pcs,
    #        pgs = plinkScore.score
    #}

    output {
        String cohortName = cohortName
        Array[File] cohortBinaries = full.binaries

        File PCs = plinkPCA.PCs
        File eigenvalues = plinkPCA.eigenvalues

        Directory scoreDir = plinkScore.arrays
        Array[File] PGscores = plinkScore.sscores
    }
}

task bcfMerge {
    input {
      String setID
      Array[File] vcfs
      
      Int memory = 2
      Int cpus = 6
      Int preemptible = 3
    }
    
    command <<<
      bcftools merge -Ob -o "~{setID}.bcf.gz" --write-index ~{sep=" " vcfs} --threads ~{cpus}
    >>>

    output {
        File mergedVcf = "~{setID}.bcf.gz"
        File index = "~{setID}.bcf.gz.csi"
        File resources = "resource_usage.log"
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory}GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/bcftools:1.18--h8b25389_0"
    }
}

task plinkMerge {
    input {
        String base
        File newBcf
        
        File oldBed
        File oldBim
        File oldFam
        
        String commonFilters = ""

        Int memory = 256
        Int cpus = 8
        Int preemptible = 3
    }

    Int diskSpace = ceil(2.2 * (size(newBcf, 'Gi') + size(oldBed, 'Gi') + size(oldBim, 'Gi') + size(oldFam, 'Gi')) )
    Int plinkMem = ceil(0.95 * memory * 1024)

    command <<<
      plink2 \
      --bcf ~{newBcf} \
      --pmerge ~{bed} ~{bim} ~{fam} \
      --set-all-var-ids "@_#_\$r_\$a" \
      --rm-dup exclude-all \
      --make-pgen \
      --out ~{base} \
      --memory ~{plinkMem} \
      ~{commonFilters}
    >>>

    output {
        Array[File] binaries = ["~{base}.pgen", "~{base}.pvar", "~{base}.psam"]
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory} GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/plink2:2.00a5--h4ac6f70_0"
    }
}

task plinkPCA {
    input {
        String base
        Array[File] binaries

        Int pcs = 10
        String flags = ""

        Int memory = 256
        Int cpus = 16
        Int preemptible = 3
    }

    Int binSize = size(binaries[0], 'Gi') + size(binaries[1], 'Gi') + size(binaries[2], 'Gi')
    Int diskSpace = ceil(2 * binSize)
    Int plinkMem = ceil(0.95 * memory * 1024)

    command <<<
      plink2 \
      --pfile ~{base} \
      --indep-pairwise 1000kb 0.1 \
      --memory ~{plinkMem} \
      ~{flags}
      
      plink2 \
      --pfile ~{base} \
      --extract ~{base}.prune.in \
      --pca ~{pcs} \
      --memory ~{plinkMem} \
      ~{flags}
    >>>

    output {
        File PCs = "~{base}.eigenvec"
        File eigenvalues = "~{base}.eigenval"
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory} GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/plink2:2.00a5--h4ac6f70_0"
    }
}

task plinkScore {
    input {
        String base
        Array[File] binaries
        
        Array[String]+ pgsNames
        Array[File]+ pgsFiles

        String flags = ""

        Int memory = 256
        Int cpus = 8
        Int preemptible = 2
    }
    
    Int binSize = size(binaries[0], 'Gi') + size(binaries[1], 'Gi') + size(binaries[2], 'Gi')
    Int diskSpace = ceil(2 * binSize)
    Int plinkMem = ceil(memory * 1024 * 0.9)
    
    command <<<
        names=(~{sep=" " pgsNames})
        files=(~{sep=" " pgsFiles})
        n=${#names[@]}
        
        mkdir arrays
        
        for i in $(seq 0 $((n-1)))
        do
          outn=~{base}_${names[$i]}
          plink2 \
          --pfile ~{base} \
          --score ${files[$i]} no-mean-imputation ignore-dup-ids \
          --out $outn \
          --memory ~{plinkMem} \
          ~{flags}
          
          mv $outn.sscore arrays
          echo arrays/$outn.sscore >> sscore_list.txt
        done
    >>>

    output {
        Directory arrays = "arrays"
        Array[File] sscores = read_lines("sscore_list.txt")
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory} GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/plink2:2.00a5--h4ac6f70_0"
    }
}