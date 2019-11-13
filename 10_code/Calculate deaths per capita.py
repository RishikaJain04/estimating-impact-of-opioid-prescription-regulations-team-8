import os
import pandas as pd

#Import the merged dataset
data = pd.read_parquet("../../merged_data.gzip")
data.sample(2)
