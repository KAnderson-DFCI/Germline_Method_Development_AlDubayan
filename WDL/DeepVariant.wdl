version 1.0

workflow DeepVariant {
    input {
        String sample
        File? captureInterval_list
        File? captureBed
    }

    parameter_meta {
        sample: "sample name"
        captureInterval_list: "WES target intervals in Picard interval list format (required if captureBed omitted)"
        captureBed: "WES target intervals in BED format (takes precedence over captureInterval_list)"
    }

    if (!defined(captureBed)) {
        File interval_list = select_first([captureInterval_list,])
        call interval_listToBed {
            input:
                interval_list=interval_list
        }
    }

    File bed = select_first([captureBed, interval_listToBed.bed])
    call deepVariant {
        input:
            sample=sample,
            captureBed=bed
    }

    call indexAndFilterVCF as ivcf {
        input:
            sample=sample,
            vcf=deepVariant.vcf
    }

    call indexAndFilterVCF as igvcf {
        input:
            sample="~{sample}.g",
            vcf=deepVariant.gvcf
    }

    output {
        File vcf = deepVariant.vcf
        File vcfIndex = ivcf.vcfIndex
        File filteredVcf = ivcf.filteredVcf
        File filteredVcfIndex = ivcf.filteredVcfIndex

        File gvcf = deepVariant.gvcf
        File gvcfIndex = igvcf.vcfIndex
        File filteredGvcf = igvcf.filteredVcf
        File filteredGvcfIndex = igvcf.filteredVcfIndex

        File visualReport = deepVariant.report
    }
}

task interval_listToBed {
    input {
        File interval_list
    }

    String bed_path = sub(basename(interval_list), "interval_list", "bed")

    command <<<
    set -xeuo pipefail

    # interval lists have headers that need to be removed and are 1-indexed, end inclusive
    # see also https://www.biostars.org/p/84686/
    grep -v "^@" ~{interval_list} \
    | awk -v OFS="\t" "{print $1, $2 - 1, $3}" \
    | sort -k1,1 -k2,2n -k3,3n \
    | bedtools merge \
    > ~{bed_path}
    >>>

    output {
        File bed = "~{bed_path}"
    }

    runtime {
        disks: "local-disk 1 HDD"
        memory: "1 GB"
        preemptible: 3
        docker: "quay.io/biocontainers/bedtools:2.28.0--hdf88d34_0"
    }
}

task deepVariant {
    input {
        String sample
        File bam
        File bai
        String model_type = "WES"
        File captureBed
        File refFasta
        File refFastaIndex

        Int cpus = 16
        String docker = "google/deepvariant:1.5.0"
        Int preemptible = 3
    }

    Int memory = ceil(1.1 * cpus)
    Int diskSize = ceil(1.15 * (size(refFasta, "G") + size(bam, "G"))) + 3

    command <<<
    set -xeuo pipefail

    mkdir deepvariant_tmp

    /opt/deepvariant/bin/run_deepvariant \
        --model_type=~{model_type} \
        --ref=~{refFasta} \
        --reads=~{bam} \
        --regions=~{captureBed} \
        --intermediate_results_dir=deepvariant_tmp \
        --output_vcf=~{sample}.vcf.gz \
        --output_gvcf=~{sample}.g.vcf.gz \
        --num_shards=~{cpus}
    >>>

    output {
        File vcf = "~{sample}.vcf.gz"
        File gvcf = "~{sample}.g.vcf.gz"
        File report = "~{sample}.visual_report.html"
    }

    runtime {
        disks: "local-disk ~{diskSize} SSD"
        memory: "~{memory} GB"
        cpu: "~{cpus}"
        preemptible: "~{preemptible}"
        docker: "~{docker}"
    }
}

task indexAndFilterVCF {
    input {
        String sample
        File vcf
    }

    command <<<
    set -xeuo pipefail

    bcftools index --tbi ~{vcf}

    # create version of VCF with only PASSing variants
    bcftools view -Oz -o ~{sample}_filtered.vcf.gz -f PASS ~{vcf}
    bcftools index --tbi ~{sample}_filtered.vcf.gz
    >>>

    output {
        File vcfIndex = "~{sample}.vcf.gz.tbi"
        File filteredVcf = "~{sample}_filtered.vcf.gz"
        File filteredVcfIndex = "~{sample}_filtered.vcf.gz.tbi"
    }

    runtime {
        disks: "local-disk 1 HDD"
        memory: "1 GB"
        preemptible: 3
        docker: "quay.io/biocontainers/bcftools:1.9--ha228f0b_3"
    }
}