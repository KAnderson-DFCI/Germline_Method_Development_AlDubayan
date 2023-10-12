task {
    input {
        String in1
        String my_repo
    }

    command <<<
    git clone ~{my_repo}

    python ./my_repo/script.py ~{in1} "output.txt"
    >>>

    output {
        File out = "output.txt"
    }

    runtime {
        disks: "local-disk 1 HDD"
        memory: "1 GB"
        cpu: "1"
        docker: "<pythondocker>"
    }
}