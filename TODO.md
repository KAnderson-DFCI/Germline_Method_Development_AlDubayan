<link href="mdext.css" rel="stylesheet"/>

# Sample Scale
- <babr> MosaicForecast?
- <babr> GCNV?

# Cohort Scale

## PCA, PRS, OLS
- <done>update sample mode of [hail_pca.py](multi_ancestry_prs/PCA/hail_pca.py) to new `stamp` and `stage` conventions
  - optimize terra runtime to minimize cost per sample
  - make workflow to aggregate PCA tsv's for sample set
- <prio>update hail wdls ([reference](WDL/Hail_BuildReference.wdl), [sample](WDL/Hail_InferSamples.wdl)) for new argument forms (--sample-vcf and --vcf-list) and processing (alternate outputs of singular and multi-vcf pca results)
- <done> test the few imputed vcfs in [Baihe's IO set](https://app.terra.bio/#workspaces/DFCI-aldubayan-lab/BSun_R2000_all_IO_normal/data) on PCA
- look into the processing of imputed vcfs from [Breast Cancer cohort](https://app.terra.bio/#workspaces/vanallen-aldubayan-lab/arab_breast_cancer/data)
  - figure out how to get submission input/output data_model mapping
  - build submission analyzer to display data procession
- figure out vcf_stats issue with certain populations
- <quick>update [dev space](https://app.terra.bio/#workspaces/DFCI-aldubayan-lab/KAnderson_Qatari_WGS_Ref_Curation/data) description with test_case sources and pca processings 

## VEP
- <babr> wire Ryan's WDL for desired annotations

## Downstream
- <babr> counting scripts?

## UKBB
- <extern> Set up billing organization

# Storage Management

- <prio>Build out tool to support nested arrays
  - Archive arab_breast_cancer_study_wes
- Rearrange to delay entity update planning until after migration
  - avoids preplanning on files that fail to be found/moved
- <quick>Export file migration issues in tsv format
- Add option to delete submission folder afterward
- Obtain and store more object size information
  - In the spreadsheet for each google project or workspace bucket
  - At the workspace level in the archive in some `sizes.tsv` by type, entity, and column
- Find where to get workspace tags from
  - Devise tags to identify archivable and cleanable workspaces
  - Can I make a scheduled vm to spin up and check weekly?
- Fill out missing details in spreadsheet
  - <extern> workspace descriptions
  - <prio> storage status
  - figure out deleted
