version 1.0

workflow CalculatePRS {
    input {
        File input_vcf
        File input_score_file
    }

    call PlinkPRS {
        input:
            input_vcf = input_vcf,
            input_score_file = input_score_file
    }

    output {
        File prs_file =  PlinkPRS.output_prs_file
        File bed_file = PlinkPRS.output_bed_file
        File bim_file = PlinkPRS.output_bim_file
        File fam_file = PlinkPRS.output_fam_file
        File no_pred_file = PlinkPRS.output_no_pred
        File no_sex_file = PlinkPRS.output_no_sex
        String prs = PlinkPRS.prs
    }
}

task PlinkPRS {
    input {
        File input_vcf
        File input_score_file
        String bcf_flag = "--bcf"
        String plink_file_prefix = "MID"

        Int memoryGb = 16
        Int diskspaceGB = 2048
        Int preemptible = 0
        Int cpu = 8
        Int retries = 3
        
        String memory_flag = ""
    }
    
    command <<<
        # Create plink files (BED,BIM,FAM) from vcf
        plink ~{bcf_flag} ~{input_vcf} --out ~{plink_file_prefix}

        # Identify Duplicate Ids
        echo "Identifying Duplicate Variant Ids"
        cut -f 2 ~{plink_file_prefix}.bim | sort | uniq -d > duplicate_ids.txt
        
        # Perform PRS calculation
        echo "Performing PRS Calculation"
        plink \
        --bfile ~{plink_file_prefix} \
        --score ~{input_score_file} 1 2 3 header \
        --exclude duplicate_ids.txt \
        --out ~{plink_file_prefix} \
        --score-no-mean-imputation \
        ~{memory_flag} \
        
        awk '{print $6}' ~{plink_file_prefix}.profile | tail -n 1 > score.txt
    >>>

    output {
        File output_prs_file = "${plink_file_prefix}.profile"
        File output_bed_file = "${plink_file_prefix}.bed"
        File output_bim_file = "${plink_file_prefix}.bim"
        File output_fam_file = "${plink_file_prefix}.fam"
        File output_no_pred = "${plink_file_prefix}.nopred"
        File output_no_sex = "${plink_file_prefix}.nosex"
        String prs = read_string("score.txt")
    }

    runtime {
        docker: "quay.io/biocontainers/plink:1.90b6.21--h779adbc_1"
        memory: "${memoryGb} GB"
        disks: "local-disk ${diskspaceGB} HDD"
        preemptible: "${preemptible}"
        cpu: "${cpu}"
        maxRetries: retries
    }

}