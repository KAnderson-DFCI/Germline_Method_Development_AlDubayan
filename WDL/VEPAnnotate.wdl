version development

workflow VEPAnnotate {
	input {
        String sampleSetId
        File vcf
        File refFasta
        File refFastaFai
        File refFastaDict
    }

    call GATKVariantsToTable {
        input:
            sampleSetId=sampleSetId,
            vcf=vcf,
            refFasta=refFasta,
            refFastaFai=refFastaFai,
            refFastaDict=refFastaDict
    }

    call vep {
        input:
            sampleSetId=sampleSetId,
            vcf=vcf,
            refFasta=refFasta,
            refFastaFai=refFastaFai,
            refFastaDict=refFastaDict
    }

    call combineOutputFiles {
        input:
            sampleSetId=sampleSetId,
            vepOutputFile=vep.annotated_tsv,
            gatkOutputFile=GATKVariantsToTable.variant_table
    }

    output {
        File vep_annotated_tsv = vep.annotated_tsv
        File vep_summary = vep.summary
        File gatk_variant_table = GATKVariantsToTable.variant_table
        File vep_annotation_list = combineOutputFiles.vep_annotation_list
        File combined_tsv = combineOutputFiles.combined_tsv
    }
}

task GATKVariantsToTable {
    input {
        String sampleSetId
        File vcf
        File refFasta
        File refFastaDict
        File refFastaFai

        Int diskSize = 100
        Int memory = 4
        Int preemptible = 3
    }
    
    command <<<
      mv ~{vcf} vcfFile.vcf.gz
      bgzip --decompress vcfFile.vcf.gz
      
      echo "Extract variants into a table format"
      java -jar /usr/GenomeAnalysisTK.jar -R ~{refFasta} -T VariantsToTable \
      -V vcfFile.vcf -o ~{sampleSetId}.variant_table.txt \
      -F CHROM -F POS -F ID -F REF -F ALT -F QUAL -F FILTER \
      -GF AD -GF DP -GF GQ -GF VAF -GF PL -GF GT --allowMissingData --showFiltered 

      echo "Number of GATK variants"
      cat ~{sampleSetId}.variant_table.txt | wc -l
      
      gzip ~{sampleSetId}.variant_table.txt
    >>>
    
    output {
        File variant_table="~{sampleSetId}.variant_table.txt.gz"
    }
    
    runtime {
        disks: "local-disk ~{diskSize} HDD"
        memory: "~{memory} GB"
        cpu: 1
        preemptible: preemptible
        docker: "vanallenlab/gatk3.7_with_htslib1.9:1.0"
    }
}

task vep {
    input {
        String sampleSetId
        File vcf
        
        Int nearestGeneDistance = 10000
        
        File refFasta
        File refFastaDict
        File refFastaFai

        Int fork = 8

        # Cache file
        File speciesCacheArchive
        
        # Plugin files
        # dbNSFP
        File dbNSFPData
        File dbNSFPDataTbi
        File dbNSFPPlugin
        # CLINVAR
        File clinvar
        File clinvar_index

        Int diskSize = 100
        Int memory = 32
        Int cpu = 16
        Int preemptible = 3
    }
    
    command <<<
        set -xeuo pipefail

        echo "Prep Annotation Data Directories"
        mkdir vep/Plugins
        mkdir vep/Plugins/data
        
        echo "Arrange Data"

        # dbNSFP
        echo "-dbNSFP"
        mv ~{dbNSFPPlugin} vep/Plugins/dbNSFP.pm
        mv ~{dbNSFPData} vep/Plugins/data/dbNSFP.gz
        mv ~{dbNSFPDataTbi} vep/Plugins/data/dbNSFP.gz.tbi
        
        echo "-Species cache"
        tar xzf ~{speciesCacheArchive} -C .
        mv homo_sapiens_merged vep
    
        # log progress to make sure that the VEP output is being generated
        function runtimeInfo() {            
            echo [$(date)]
            echo \* CPU usage: $(top -bn 2 -d 0.01 | grep '^%Cpu' | tail -n 1 | awk '{print $2}')%
            echo \* Memory usage: $(free -m | grep Mem | awk '{ OFMT="%.0f"; print ($3/$2)*100; }')%
            echo \* Disk usage: $(df | grep cromwell_root | awk '{ print $5 }')
        }
        while true;
            do runtimeInfo;
            sleep 30;
        done &
        
        # Run VEP
        # --everything covers
        #   --sift b, --poly--sift b, --polyphen b, --ccds, --hgvs, --symbol, --numbers,
        #   --domains, --regulatory, --canonical, --protein, --biotype, --af, --af_1kg,
        #   --af_esp, --af_gnomade, --af_gnomadg, --max_af, --pubmed, --uniprot, --mane,
        #   --tsl, --appris, --variant_class, --gene_phenotype, --mirna
        echo "running VEP..."
        vep -v -i ~{vcf} -o ~{sampleSetId}.vep \
        --tab --force_overwrite --stats_text \
        --fork ~{fork} --offline --cache --merged --dir vep \
        --fasta ~{refFasta} --use_given_ref ~{refFasta} \
        --pick --everything --total_length --hgvsg \
        --nearest gene --distance ~{nearestGeneDistance} \
        --plugin dbNSFP,vep/Plugins/data/dbNSFP.gz,ExAC_Adj_AC,ExAC_nonTCGA_Adj_AC,ExAC_nonTCGA_Adj_AF,ExAC_Adj_AF,gnomAD_genomes_AC,gnomAD_genomes_AN,gnomAD_genomes_AF,REVEL_score,M-CAP_score,MetaSVM_score,MetaLR_score,GenoCanyon_score,integrated_fitCons_score,clinvar_rs,Interpro_domain,GTEx_V6p_gene,GTEx_V6p_tissue \
        --custom ~{clinvar},ClinVar_Jan2024,vcf,exact,0,ID,ALLELEID,CLNDN,CLNDISDB,CLNHGVS,CLNREVSTAT,CLNSIG,CLNSIGCONF,CLNVI,DBVARID
        
        echo "Number of VEP variants:"
        grep -v "#" ~{sampleSetId}.vep | wc -l
        
        gzip ~{sampleSetId}.vep
    >>>
    
    output {
        File annotated_tsv = "~{sampleSetId}.vep.gz"
        File summary = "~{sampleSetId}.vep_summary.txt"
    }

    runtime {
        disks          : "local-disk ~{diskSize} SSD"
        bootDiskSizeGb : "100 GB"
        memory         : "~{memory} GB"
        cpu            : cpu
        docker         : "ensemblorg/ensembl-vep:release_111.0"
        preemptible    : preemptible
    }

}

task combineOutputFiles {
    input {
        String sampleSetId
        File vepOutputFile
        File gatkOutputFile
        
        Int diskSize = 200
    }

    command <<<
      mv ~{vepOutputFile} vepOutputFile.txt.gz
      mv ~{gatkOutputFile} gatkOutputFile.txt.gz
    
      gunzip vepOutputFile.txt.gz
      gunzip gatkOutputFile.txt.gz
    
      echo "Removing a # sign from the column header of the VEP output file"
      sed "s/#Uploaded_variation/Uploaded_variation/g" vepOutputFile.txt > ~{sampleSetId}_VEP_temp.txt

      # Separate the header meta from the data
      grep "#" ~{sampleSetId}_VEP_temp.txt > ~{sampleSetId}_VEP_annotation_list.txt
      grep -v "##" ~{sampleSetId}_VEP_temp.txt > ~{sampleSetId}_VEP.txt

      echo "Number of VEP variants"
      cat ~{sampleSetId}_VEP.txt | wc -l
      
      echo "Combining VEP and GATK outputs"
      paste ~{sampleSetId}_VEP.txt gatkOutputFile.txt | bgzip > ~{sampleSetId}_VEP_Genotypes.txt.gz
    >>>
    
    output {
        File combined_tsv = "~{sampleSetId}_VEP_Genotypes.txt.gz"
        File vep_annotation_list = "~{sampleSetId}_VEP_annotation_list.txt"
    }
    
    runtime {
        disks        : "local-disk ${diskSize} HDD"
        memory       : "14 GB"
        cpu          : 1
        preemptible  : 3
    }
}