from firecloud import fiss
import hail as hl

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import seaborn

# initialize hail
hl.init()


# inputs
ref_name = "1KG"
ref_vcf = "gs://fc-e4808678-a557-43c2-b46e-20385d5c64f0/0f2fdd10-7168-'4d73-94ce-b55475827599/VT_Decomp/e9cf326c-c7b9-4d26-b0c1-7f4270adc4d2/call-VTRecal/Jul2021_1000g_3202_WGS_DV_merged.vt2_normalized_spanning_alleles.vcf.gz"
ref_clinical_info = "gs://fc-86adf333-cc1a-4027-9e69-fd0433d63d1c/hg38_references/supercohort_combined_with_info.txt"


ref_mt = hl.import_vcf(ref_vcf, n_partitions=200, reference_genome='GRCh38', force_bgz=True, array_elements_required=False)
merged_mt = ref_mt.annotate_entries(GT = hl.if_else(hl.is_defined(ref_mt.GT), ref_mt.GT, hl.Call([0, 0])))

# filters
filtered_mt = hl.variant_qc(ref_mt)
filtered_mt = filtered_mt.filter_rows(filtered_mt.variant_qc.AF[1] > 0.01) # Rare variant filter
filtered_mt = filtered_mt.filter_rows(filtered_mt.variant_qc.p_value_hwe > 1e-6) # Hardy-Weinberg Equilibrium filter
variants_after_pruning = hl.ld_prune(filtered_mt.GT, r2=0.1)   # Pruning 
filtered_mt = filtered_mt.filter_rows(hl.is_defined(variants_after_pruning[filtered_mt.row_key]))

print(filtered_mt.count())

pca_eigenvalues, pca_scores, _ = hl.hwe_normalized_pca(filtered_mt.GT)