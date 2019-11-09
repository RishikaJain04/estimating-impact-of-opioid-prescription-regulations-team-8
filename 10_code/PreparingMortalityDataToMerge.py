

os.chdir("C:/Users/Felipe/Desktop/Duke MIDS/Practical Tools in Data Science/")
cod = pd.read_csv("drug_data_full.csv") #Cause of death
data_drop = cod.drop((['Drug/Alcohol Induced Cause','Notes', 'Year Code','County']), axis=1)
data_drop.rename(columns = {'County Code':'FIPS'}, inplace = True)
dd = data_drop.dropna(how='all')
mortality_data = dd[(dd['Drug/Alcohol Induced Cause Code'] != 'A9') &
(dd['Drug/Alcohol Induced Cause Code'] != 'D9') &
(dd['Drug/Alcohol Induced Cause Code'] != 'O9')]
mortality_data
