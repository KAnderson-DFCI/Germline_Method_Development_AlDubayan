version development

workflow Hail_InferSamples_Remote {
  call InferSamples
}

task InferSamples {
  input {
    String sample_vcf
    String refloadings
    File refRFmodel
    Int pop_col

    String? ref_build
    String? bucket_prefix # like gs://google-storage-bucket/

    Int diskGB = 1024
    Int memGB = 64
    Int cpu = 16
    Int preemptible = 3
    String hail_docker = "hailgenetics/hail:0.2.127-py3.11"
  }

  String samplebase = sub(basename(sample_vcf), "\\.[bv]cf\\.gz", "")

  command <<<
    python3 hail_pca.py \
      ~{"-r" + ref_build} ~{"-b" + bucket_prefix} \
      infer-samples \
      ~sample_vcf ~refloadings ~refRFmodel

  >>>

  output {
    File ref_pca_pop_table = "~{refbase}.pca_pop.tsv"
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: ~hail_docker
  }
}