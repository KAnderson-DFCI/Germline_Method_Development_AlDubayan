version development

workflow SRAToFastq {
  input {
    String accession
    File prefetch
  }

  call extract_fastq {
    input:
      accession=accession,
      prefetch=prefetch
  }

  output {
    File fq1 = extract_fastq.fq1
    File fq2 = extract_fastq.fq2
    File fq_unpaired = extract_fastq.fq_unpaired
  }
}

task extract_fastq {
  input {
    String accession
    File prefetch

    Int memGB = 12
    Int cpu = 8
    Int preemptible = 3
    String sratools_docker = "vanallenlab/sratools:3.0.0"
  }
  
  Int diskGB = ceil(20 * size(prefetch, 'GB'))

  command <<<
    vdb-config --set /repository/remote/disabled=true

    mkdir ~{accession}
    mv ~{prefetch} ~{accession}

    fasterq-dump ~{accession}
  >>>

  output {
    File fq1 = "~{accession}_1.fastq"
    File fq2 = "~{accession}_2.fastq"
    File fq_unpaired = "~{accession}.fastq"
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: sratools_docker
  }
}