version development

workflow Hail_BuildReference_Remote {
  call BuildReference
}

task BuildReference {
  input {
    String reference_vcf
    File population_tsv
    Int pop_col
    String? sample_vcf

    Int? PCs
    Float? af_min
    Float? hwe_p 
    Float? ld_r2 

    String? ref_build
    String? bucket_prefix # like gs://google-storage-bucket/

    Int diskGB = 1024
    Int memGB = 64
    Int cpu = 16
    Int preemptible = 3
    String hail_docker = "hailgenetics/hail:0.2.127-py3.11"
  }

  String refbase = sub(basename(reference_vcf), "\\.[bv]cf\\.gz", "")

  command <<<
    python3 hail_pca.py \
      ~{"-r" + ref_build} ~{"-b" + bucket_prefix} \
      build-reference \
      ~{"-k" + PCs} \
      ~{"--af-min" + af_min} ~{"--hwe-p" + hwe_p} ~{"--ld_r2" + ld_r2} \
      ~reference_vcf ~population_tsv \
      -c ~pop_col \
      ~{"-s" + sample_vcf}

  >>>

  output {
    File ref_pca_pop_table = "~{refbase}.pca_pop.tsv"
    File ref_pop_rf_model = "~{refbase}.pop_rf.sklearn.joblib"
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: ~hail_docker
  }
}