# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#%
import numpy as np
import pandas as pd

# Rrs data
rrsData = pd.read_csv('/Users/jakravit/Desktop/synthetic_rrs_1nm.csv', index_col=0)
rrs = rrsData.filter(regex='^\d').astype(float)
rrs.columns = rrs.columns.astype(float)
meta = rrsData.filter(regex='^[a-zA-Z]')

# cyanosat SRF - full
cs_srf12 = pd.read_csv('/Users/jakravit/Desktop/cyanosat/SRFs/CyanoSat_SRF_Idealised_12nm.csv', index_col=0)
cs_srf8 = pd.read_csv('/Users/jakravit/Desktop/cyanosat/SRFs/CyanoSat_SRF_Idealised_8nm.csv', index_col=0)
cs_srf12.plot(legend=False)
cs_srf8.plot(legend=False)

# # SRF - 8 nm 
# cs_srf8 = cs_srf.iloc[:,::8] # this is wrong, not sure how they did their resampling
# cs_srf8.plot(legend=False)

# # SRF - 12 nm 
# cs_srf12 = cs_srf.iloc[:,::5] # this equals the 12nm resampling which josh sent in email (61 bands between 500-800)
# cs_srf12.plot(legend=False)

# thuiller solar
solar = pd.read_csv('thuillier.csv',index_col='wl')

# function for resampling
def srfCalc (spec, solar, srf):
    name = spec.name
    merged = pd.merge(pd.merge(srf, solar,right_index=True, left_index=True), spec, right_index=True, left_index=True)
    rrs_res = []
    for l in srf.columns.values:        
        merged['numerator'] = merged.apply(lambda row: (row[l]*row[name]*row['Ed']), axis=1)
        merged['denominator'] = merged.apply(lambda row: (row[l]*row['Ed']), axis=1)
        rrs_res.append(merged.numerator.sum()/merged.denominator.sum())
    return rrs_res

#%%
# resample at 12nm res

print ('Running...')
new_rrs = rrs.T.apply(lambda row: srfCalc(row,solar,cs_srf12))
new_rrs = new_rrs.T
new_rrs.columns = cs_srf12.columns
final = pd.concat([meta, new_rrs], axis=1)
# final.to_csv('/Users/jakravit/Desktop/cyanosat_12nm_synthetic_rrs.csv')

#%%
import matplotlib.pyplot as plt

rrs10  = rrs.iloc[:10,:]
rrs2 = rrs10.loc[:, 500:800]

newrrs12 = rrs10.T.apply(lambda row: srfCalc(row,solar,cs_srf12))
newrrs8 = rrs10.T.apply(lambda row: srfCalc(row,solar,cs_srf8))

fig, (ax1, ax2, ax3) = plt.subplots(1,3, figsize=(16,4))
rrs2.T.plot(ax=ax1, legend=False, title='Synthetic 1nm')
newrrs8.plot(ax=ax2, legend=False, title='CyanoSat 8nm')
newrrs12.plot(ax=ax3, legend=False, title='CyanoSat 12nm')
