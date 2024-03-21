import sys
from os.path import basename
import re

from joblib import dump, load

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score

import hail as hl
hl.init()


# TODO make configurable
nPCs = 15
def PCcols(n):
    return [f'PC{i}' for i in range(1, n+1)]

# inputs
def run(args):
    if args[0] == '--build-reference':
        refvcf, refpoptsv = args[1:3]
        build_reference(refvcf, refpoptsv)
    elif args[0] == '--infer-samples':
        samplevcf, refloadingsfile, refRFdump = args[1:4]
        infer_samples(samplevcf, refloadingsfile, refRFdump)
    elif args[0] == '--build-and-infer':
        samplevcf, refvcf, refpoptsv = args[1:4]
        ref_loadings_ht, rf = build_reference(refvcf, refpoptsv)
        infer_samples(samplevcf, ref_loadings_ht, rf, need_load=False)
    else:
        print("Invalid paramters") #TODO make better instructions.


## build reference projection
# needs reference vcf, reference populations
def build_reference(refvcf, refpoptsv):
    # hail can't take '.bcf' files anyway, but let's keep it portable
    refbase = re.sub("\.[bv]cf\.gz", "", basename(refvcf))
    
    # perform pca
    ref_mt = hl.import_vcf(refvcf, force_bgz=refvcf.endswith('.gz'), reference_genome="GRCh38") #TODO make ref configurable
    ref_mt = prep_mt_pca(ref_mt) # atomizing should help with garbage collection
    _, ref_pcs_ht, ref_loadings_ht = hl.hwe_normalized_pca(ref_mt.GT, k=nPCs, compute_loadings=True)
    
    # annotate loadings with allele frequencies
    ref_mt = ref_mt.annotate_rows(af = hl.agg.mean(ref_mt.GT.n_alt_alleles()) / 2)                
    ref_loadings_ht = ref_loadings_ht.annotate(af=ref_mt.rows()[ref_loadings_ht.key].af)         

    ### output loadings for reuse
    ref_loadings_ht.write(f'{refbase}.loadings.ht') #TODO this needs to be output locally, not in hadoop, for terra delocalization

    PCs = PCcols(nPCs)
    # prepare model data
    ref_data = ref_pcs_ht.to_pandas()
    ref_data.set_index('s', inplace=True)
    ref_data.index.name = 'Sample'
    ref_data = ref_data['scores'].apply(lambda x: pd.Series(x, index = PCs))

    ref_pops = pd.read_csv(refpoptsv, sep='\t', index_col=[0]) # TODO Make population column configurable?
    ref_data['Population'] = ref_pops.loc[ref_data.index]

    ### output pca with populations for inspection
    ref_data.to_csv(f'{refbase}.pca_pop.tsv')

    # train random forest
    tX = ref_data[PCs]
    ty = ref_data['Population']
    rf = RandomForestClassifier()
    rf_cvscores = cross_val_score(rf, tX, ty, cv=5)
    print(rf_cvscores) # emit for inspection
    rf.fit(tX, ty)

    ### save random forest model for reuse
    dump(f'{refbase}.pop_sklearn_rf.joblib')

    return ref_loadings_ht, rf

#TODO come up with elegant configuration input
def prep_mt_pca(mt, aft=0.01, hwe_pt=1e-6, ld_r2=0.1):
    print("Filtering", mt.count(), 'variants.')

    # missing calls > ref
    filled_gt = hl.if_else(hl.is_defined(mt.GT), mt.GT, hl.Call([0, 0]))
    mt = mt.annotate_entries(GT=filled_gt)
    mt = hl.variant_qc(mt)

    # remove rare variants
    mt = mt.filter_rows(mt.variant_qc.AF[1] > aft)   
    
    # remove variants out of hardy weinberg equilibrium
    mt = mt.filter_rows(mt.variant_qc.p_value_hwe > hwe_pt)

    # remove variants in linkage disequilibrium
    ld_keep = hl.ld_prune(mt.GT, r2=ld_r2)
    mt = mt.filter_rows(hl.isdefined(ld_keep[mt.row_key]))
    
    print(mt.count(), 'variants remaining.')
    return mt


def infer_samples(samplevcf, refloadings, refRF, need_load=True):
    samplebase = re.sub("\.[bv]cf\.gz", "", basename(samplevcf))

    if need_load:
        ref_loadings_ht = hl.read_table(refloadings)
        rf = load(refRF)
    else:
        ref_loadings_ht = refloadings
        rf = refRF

    if rf.n_features_in_ != nPCs: #TODO compare rf to loadings length for PC number, overriding nPCs
        print(f"Random Forest designed for {rf.n_features_in_} PCs, but script configured for {nPCs}.")

    sample_mt = hl.import_vcf(samplevcf, force_bgz=samplevcf.endswith('.gz'), reference_genome='GRCh38') #TODO configurate

    # project samples using reference loadings
    sample_pcs_ht = hl.experimental.pc_project(sample_mt.GT, ref_loadings_ht.loadings, ref_loadings_ht.af)

    PCs = PCcols(nPCs)
    # prepare model data
    sample_data = sample_pcs_ht.to_pandas()
    sample_data.set_index('s', inplace=True)
    sample_data.index.name = 'Sample'
    sample_data = sample_data['scores'].apply(lambda x: pd.Series(x, index=PCs))

    sample_data['Population'] = rf.predict(sample_data[PCs])
    
    ### Output results
    sample_data.to_csv(f'{samplebase}.pca_pop.tsv', sep='\t')
