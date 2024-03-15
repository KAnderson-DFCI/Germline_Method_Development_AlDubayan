version development

workflow VCFStatsStreamed {
  input {
    String remote_vcf_path
    File vcf_idx
    String sampleId
  }

  call vcf_stats_streamed {
    input:
      remote_vcf_path = remote_vcf_path,
      vcf_idx = vcf_idx,
      pref = sampleId
  }

  output {
    File vcf_stats = vcf_stats_streamed.stats
  }
}

task vcf_stats_streamed {
  input {
    String remote_vcf_path
    File vcf_idx
    String? pref

    String bcftools_docker = "kylera/samtools-suite:gcloud"

    Int diskGB = 256
    Int memGB = 8
    Int cpu = 4
    Int preemptible = 3
  }

  String base = if defined(pref) then pref else sub(basename(remote_vcf_path), "\\.[bv]cf(\\.gz)?^", "")

  command <<<
    set -eu -o pipefail
    export GCS_OAUTH_TOKEN=`gcloud auth application-default print-access-token`

    mv ~{vcf_idx} .

    bcftools stats \
      --threads ~{cpu} \
      -s - \
      ~{remote_vcf_path} \
      > ~{base}.stats.txt
  >>>

  output {
    File stats = "~{base}.stats.txt"
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: bcftools_docker
  }
}