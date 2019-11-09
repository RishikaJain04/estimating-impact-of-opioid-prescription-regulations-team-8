### In this script, we merge two datasets:
### (1) Shipments of drugs for each county
### (2) Population of each county

import os
import pandas as pd
import numpy as np

#Open populations file
os.chdir("C:/Users/Felipe/Desktop/Duke MIDS/Practical Tools in Data Science/estimating-impact-of-opioid-prescription-regulations-team-8/20_intermediate_files")
population = pd.read_csv("population data with FIP.csv", encoding='latin-1')

#Select relevant columns
population = population.loc[:,['FIP',
                               'POPESTIMATE2010',
                              'POPESTIMATE2011',
                              'POPESTIMATE2012',
                              'POPESTIMATE2013',
                              'POPESTIMATE2014',
                              'POPESTIMATE2015',
                              'POPESTIMATE2016',
                              'POPESTIMATE2017',
                              'POPESTIMATE2018']]

#Open shipments file
os.chdir("C:/Users/Felipe/Desktop/Duke MIDS/Practical Tools in Data Science/")
shipments = pd.read_parquet("shipments_with_FIPS.gzip")

#Rename population column to match shipment
population.rename(columns = {'FIP':'FIPS'}, inplace = True)

#Adjust shipment FIPS to numpy int64 for matching
shipments.FIPS = np.int64(shipments.FIPS)

assert type(shipments.FIPS[0]) == type(population.FIPS[0])

#Merge
data = pd.merge(shipments, population, on = 'FIPS', how = 'inner', indicator=True, validate='m:1')

#Check if merge was succesful
assert len(data.loc[data._merge != 'both']) == 0, "Some counties were only present in one dataset, not on both"

