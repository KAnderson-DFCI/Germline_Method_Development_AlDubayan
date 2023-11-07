version 1.0

workflow PlinkPCA {
    input {
    	String cohortName
        File plinkBed
        File plinkBim
        File plinkFam
    }

    call plinkScore {
        input:
          base = cohortName,
          bed = plinkBed,
          bim = plinkBim,
          fam = plinkFam
    }

    output {
        PCs = plinkPCA.PCs
        eigenvalues = plinkPCA.eigenvalues
        variantWeights = plinkPCA.variantWeights
    }
}

task plinkPCA {
    input {
        String base
        File bed
        File bim
        File fam

        Int pcs = 20
        String flags = ""

        Int memory = 256
        Int cpus = 8
        Int preemptible = 3
    }

    Int diskSpace = ceil(2 * size(bed, 'Gi'))

    command <<<
      plink \
      --bfile ~{base} \
      --pca ~{pcs} header tabs var-wts\ 
      ~{flags}
    >>>

    output {
        File PCs = "{base}.eigenvec"
        File eigenvalues = "{base}.eigenval"
        File variantWeights = "{base}.eigenvec.var"
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory} GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/plink:1.90b6.21--h779adbc_1"
    }
}