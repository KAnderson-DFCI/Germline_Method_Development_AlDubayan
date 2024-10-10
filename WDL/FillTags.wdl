version = development

workflow FillTags {
  input {
    File vcf
    File vcf_idx
  }

  call filltags {
    input:
      vcf = vcf,
      vcf_idx = vcf_idx
  }

  output {
    File vcf_with_counts = filltags.vcf_with_counts
    File idx = filltags.idx
  }
}


task filltags {
  input {
    File vcf
    File vcf_idx

    String outf = 'z'
    Boolean otbi = false

    Int memGB = 12
    Int cpu = 6
    Int preemptible = 3

    String bcftools_docker = "kylera/samtools-suite:gcloud"
  }

  Int diskGB = ceil( 2.5 * size([vcf, vcf_idx], 'GB'))

  String here_vcf = basename(vcf)
  String here_idx = basename(vcf_idx)

  String suf = if outFormat == "v" then ".vcf" else (
                 if outFormat == "z" then ".vcf.gz" else (
                 if outFormat == "u" then ".bcf" else (
                 if outFormat == "b" then ".bcf.gz" else ".vcf")))
  String outn = sub(here_vcf, "\\.[bv]cf(\\.gz)?", "") + ".counted." + outFormat

  command <<<

  ln -s ~{vcf} ~{here_vcf}
  ln -s ~{vcf_idx} ~{here_idx}

  bcftools +fill-tags ~{here_vcf} --threads ~{cpu} -O~{outFormat} -o ~{outn}

  bcftools index ~{if tbi then "-t" else ""} --threads ~{cpu} ~{outn}
  >>>

  output {
    File vcf_with_counts = outn
    File idx = "~{outn}.~{if tbi then "tbi" else "csi"}"
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: bcftools_docker
  }
}