version development

workflow VariantQC {
  input {
    File vcf
    File vcf_idx
  }

  call variantqc {
    input:
      vcf = vcf,
      vcf_idx = vcf_idx
  }

  output {
    File vqc_report = variantqc.html_report
    File vqc_json = variantqc.json_report
  }
}

task variantqc {
  input {
    File vcf
    File vcf_idx

    File ref_fasta
    File? pedigree

    Int memGB = 16
    Int cpu = 4
    Int preemptible = 3
    String docker = "ghcr.io/bimberlab/discvrseq:latest"
  }

  Int diskGB = ceil(2 * size(vcf)) + 10

  String here_vcf = basename(vcf)
  String base = sub(here_vcf, "\\.[bv]cf(\\.gz)?", "")

  command <<<
    set -e -o pipefail

    ln ~{vcf} .
    ln ~{vcf_idx} .

    java -jar DISCVRSeq.jar VariantQC \
     -R ~{ref_fasta} \
     -ped ~{pedigree} \
     -V ~{here_vcf} \
     -O ~{base}.vqc.html \
     -rd ~{base}.vqc.json \
     --threads ~{cpu}
  >>>

  output {
    File html_report = "~{base}.vqc.html"
    File json_report = "~{base}.vqc.json"
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: docker
  }
}