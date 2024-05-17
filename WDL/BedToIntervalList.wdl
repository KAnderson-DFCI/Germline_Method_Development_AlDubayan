version development

workflow BedToIntervalList {
  input {
    File bed
    File refDict
  }

  call bedToIntervalList {
    input:
      bed=bed,
      refDict=refDict
  }

  output {
    File interval_list = bedToIntervalList.interval_list
  }
}

task bedToIntervalList {
  input {
    File bed
    File refDict
    
    Int memGB = 8
    Int cpu = 1
    Int preemptible = 3
    String picard_docker = "broadinstitute/picard"
    String picard_path = "/usr/picard/picard.jar"
  }

  String base = sub(basename(bed), '\\.bed', '')
  Int diskGB = ceil(2 * size(bed, 'GB'))

  command <<<
    java -jar ~{picard_path} BedToIntervalList \
      I=~{bed} \
      O=~{base}.interval_list \
      SD=~{refDict}
  >>>

  output {
    File interval_list = "~{base}.interval_list"
  }

  runtime {
    disks: "local-disk ~{diskGB} HDD"
    memory: "~{memGB}GB"
    cpu: cpu
    preemptible: preemptible
    docker: picard_docker
  }
}