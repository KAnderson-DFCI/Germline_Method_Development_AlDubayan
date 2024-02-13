
version development

workflow VTNorm_Streamed {
  input {
    String sample_id
    String remote_vcf_path
    File vcf_idx

    File refFasta
    File refFastaIdx
    File refFastaDict

    File region_list
  }

  Array[String] regions = read_lines(region_list)

  scatter (i in range(length(regions))) {
    call vtnorm_streamed {
      input:
        remote_vcf_path = remote_vcf_path,
        vcf_idx = vcf_idx,
        region = regions[i],
        pref = "~{sample_id}.shard_~{i}",

        refFasta = refFasta,
        refFastaIdx = refFastaIdx,
        refFastaDict = refFastaDict
    }
  }

  call concat {
    input:
      vcfs = vtnorm_streamed.norm_vcf,
      pref = "~{sample_id}.normalized"
  }

  output {
    File norm_vcf = concat.vcf
    File? norm_vcf_idx = concat.vcf_idx
  }
}


task vtnorm_streamed {
  input {
    String remote_vcf_path
    File vcf_idx
    String? region
    String? pref
    String outFormat = "z3"
    
    File refFasta
    File refFastaIdx
    File refFastaDict

    Int diskGB = 256
    Int memGB = 8
    Int cpu = 9
    Int preemptible = 3
    String vt_bcftools_docker = "kylera/vt:latest"
    String vt_path = "/opt/vt/vt"
  }

  String base = if defined(pref) then pref else sub(basename(remote_vcf_path), "\\.[bv]cf(\\.gz)?^", "")
  
  String f = sub(outFormat, "[[:digit:]]", "")
  String suf = if f == "v" then ".vcf" else ( # most common short circuit, but also default at end
               if f == "z" then ".vcf.gz" else (
               if f == "u" then ".bcf" else (
               if f == "b" then ".bcf.gz" else ".vcf")))

  command <<<
    set -eu -o pipefail
    export GCS_OAUTH_TOKEN=`gcloud auth application-default print-access-token`

    # The pipeline
    bcftools view -Ou \
      ~{"-r " + region} \
      --threads 3 \
      ~{remote_vcf_path} | \
    ~{vt_path} decompose + -s -o + | \
    ~{vt_path} normalize + -r ~{refFasta} | \
    sed 's/*/-/g' - | \
    bcftools view \
      -O~{outFormat} \
      -o ~{base}.normalized~{suf} \
      --threads 3 \
      --write-index \
      -
  
  >>>

  output {
    File norm_vcf = "~{base}.normalized~{suf}"
    File? norm_vcf_idx = "~{base}.normalized~{suf}.csi"
  }

  runtime {
    disks: "localdisk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: vt_bcftools_docker
  }
}


task concat {
  input {
    Array[File] vcfs
    String pref
    String outFormat = "z3"
    
    Int memGB = 8
    Int cpu = 4
    Int preemptible = 3
    String bcftools_docker = "kylera/samtools-suite:latest"
  }

  Int diskGB = ceil(3* size(vcfs, "GB"))

  String f = sub(outFormat, "[[:digit:]]", "")
  String suf = if f == "v" then ".vcf" else ( # most common short circuit, but also default at end
               if f == "z" then ".vcf.gz" else (
               if f == "u" then ".bcf" else (
               if f == "b" then ".bcf.gz" else ".vcf")))

  command <<<
    bcftools concat \
      -O ~{outFormat} \
      -o ~{pref}~{suf} \
      --write-index \
      --threads {cpu} \
      --naive \
      -f ~{write_lines(vcfs)}

  >>>

  output {
    File vcf = "~{pref}~{suf}"
    File? vcf_idx = "~{pref}~{suf}.csi"
  }

  runtime {
    disks: "localdisk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: bcftools_docker
  }
}