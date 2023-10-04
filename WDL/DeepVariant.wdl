version 1.0

workflow DeepVariant {
    String sample
    File? capture_interval_list
    File? capture_bed

    parameter_meta {
        sample: 'sample name'
        capture_interval_list: 'WES target intervals in Picard interval list format (required if capture_bed omitted)'
        capture_bed: 'WES target intervals in BED format (takes precedence over capture_interval_list)'
    }

    if (!defined(capture_bed)) {
        call interval_list_to_bed {
            input:
                interval_list=capture_interval_list
        }
    }
    File? bed = if (defined(capture_bed)) then capture_bed else interval_list_to_bed.bed

    call deep_variant {
        input:
            sample=sample,
            capture_bed=bed
    }

    call bgzip as bgz1 {
        input:
            sample=sample,
            uncompressed_vcf=deep_variant.vcf
    }

    call bgzip as bgz2 {
        input:
            sample='${sample}.g',
            uncompressed_vcf=deep_variant.gvcf
    }
}

task interval_list_to_bed {
    File interval_list
    String bed_path = sub(basename(interval_list), 'interval_list', 'bed')

    command <<<
    set -xeuo pipefail

    # interval lists have headers that need to be removed and are 1-indexed
    # see also https://www.biostars.org/p/84686/
    grep -v '^@' ${interval_list} \
    | awk -v OFS='\t' '{print $1, $2 - 1, $3}' \
    | sort -k1,1 -k2,2n -k3,3n \
    | bedtools merge \
    > ${bed_path}
    >>>

    output {
        File bed = '${bed_path}'
    }

    runtime {
        memory: "1 GB"
        disks: "local-disk 1 HDD"
        preemptible: 3
        docker: "quay.io/biocontainers/bedtools:2.28.0--hdf88d34_0"
    }
}

task deep_variant {
    String sample
    File bam
    File bai
    String model_type = 'WES'
    File reference_fasta
    File reference_fasta_fai
    File capture_bed

    Int runtime_cpus
    String runtime_docker
    Int runtime_memory = ceil(1.1 * runtime_cpus)
    Int runtime_disk_buffer
    Int runtime_disk = ceil(1.15 * (size(reference_fasta, 'G') + size(bam, 'G')) + runtime_disk_buffer)
    Int runtime_preemptible

    command <<<
    set -xeuo pipefail

    mkdir deepvariant_tmp

    /opt/deepvariant/bin/run_deepvariant \
        --model_type=${model_type} \
        --ref=${reference_fasta} \
        --reads=${bam} \
        --regions=${capture_bed} \
        --intermediate_results_dir=deepvariant_tmp \
        --output_vcf=${sample}.vcf \
        --output_gvcf=${sample}.g.vcf \
        --num_shards=${runtime_cpus}
    >>>

    output {
        File vcf = "${sample}.vcf.gz"
        File gvcf = "${sample}.g.vcf.gz"
        File report = "${sample}.visual_report.html"
    }

    runtime {
        memory: "${runtime_memory} GB"
        cpu: "${runtime_cpus}"
        disks: "local-disk ${runtime_disk} SSD"
        preemptible: "${runtime_preemptible}"
        docker: "${runtime_docker}"
    }
}

task bgzip {
    String sample
    File uncompressed_vcf

    Int runtime_disk

    command <<<
    set -xeuo pipefail

    bcftools view -Oz -o ${sample}.vcf.gz ${uncompressed_vcf}
    bcftools index --tbi ${sample}.vcf.gz

    # create version of VCF with only PASSing variants
    bcftools view -Oz -o ${sample}_filtered.vcf.gz -f PASS ${uncompressed_vcf}
    bcftools index --tbi ${sample}_filtered.vcf.gz
    >>>

    output {
        File vcf = '${sample}.vcf.gz'
        File vcf_index = '${sample}.vcf.gz.tbi'
        File filtered_vcf = '${sample}_filtered.vcf.gz'
        File filtered_vcf_index = '${sample}_filtered.vcf.gz.tbi'
    }

    runtime {
        memory: '1 GB'
        disks: 'local-disk ${runtime_disk} HDD'
        preemptible: 3
        docker: 'quay.io/biocontainers/bcftools:1.9--ha228f0b_3'
    }
}