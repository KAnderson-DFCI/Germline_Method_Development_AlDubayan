version 1.0

workflow PlinkPrep {
    input {
        String base
        File vcf
        String type = "vcf"
        String input_flags = ""
    }

    call plinkPrep {
        input:
          base = base,
          vcf = vcf,
          type = type,
          flags = flags
    }

    output {
        File bed = plinkPrep.bed
        File bim = plinkPrep.bim
        File fam = plinkPrep.fam
    }
}

task plinkPrep{
    input {
        String base
        File vcf
        String type = "vcf"
        String flags = ""

        Int memory = 4
        Int cpus = 1
        Int preemptible = 3
    }

    Int diskSpace = ceil(size(vcf, "Gi"))

    command <<<
        # Creates plink files (BED, BIM, FAM) from vcf
        plink --~{type} ~{vcf} --out ~{base} ~{flags}
    >>>

    output {
        File bed = "~{base}.bed"
        File bim = "~{base}.bim"
        File fam = "~{base}.fam"
    }

    runtime {
        disks: "local-disk ~{diskSpace} HDD"
        memory: "~{memory} GB"
        cpu: cpus
        preemptible: preemptible
        docker: "quay.io/biocontainers/plink:1.90b6.21--h779adbc_1"
    }
}