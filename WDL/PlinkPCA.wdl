version 1.0

workflow PlinkPCA {
    input {
    	String cohortName
        File plinkBed
        File plinkBim
        File plinkFam
    }

    call plinkPCA {
        input:
          base = cohortName,
          bed = plinkBed,
          bim = plinkBim,
          fam = plinkFam
    }

    output {
        File PCs = plinkPCA.PCs
        File eigenvalues = plinkPCA.eigenvalues
        File variantWeights = plinkPCA.variantWeights
    }
}

task plinkPCA {
    input {
        String base
        File bed
        File bim
        File fam

        Int pcs = 10
        String flags = ""

        Int memory = 256
        Int cpus = 16
        Int preemptible = 3
    }

    Int diskSpace = ceil(2 * size(bed, 'Gi'))
    Int plinkMem = ceil(0.95 * memory * 1024)

    command <<<
    
      plink2 \
      --bfile ~{base} \
      --bed ~{bed} --bim ~{bim} --fam ~{fam} \
      --pca allele-wts ~{pcs} vzs vcols=+pos \
      --memory ~{plinkMem} \
      ~{flags}
    >>>

    output {
        File PCs = "{base}.eigenvec"
        File eigenvalues = "{base}.eigenval"
        File variantWeights = "{base}.eigenvec.allele"
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory} GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/plink2:2.00a5--h4ac6f70_0"
    }
}