

os.chdir("C:/Users/Felipe/Desktop/Duke MIDS/Practical Tools in Data Science/")
cod = pd.read_csv("drug_data_full.csv") #Cause of death
cod = drug_data_full.drop((['Drug/Alcohol Induced Cause','Notes', 'Year Code','County']), axis=1)
cod.rename(columns = {'County Code':'FIPS'}, inplace = True)
dd = cod.dropna(how='all')
mortality_data = dd[(dd['Drug/Alcohol Induced Cause Code'] != 'A9') &
(dd['Drug/Alcohol Induced Cause Code'] != 'D9') &
(dd['Drug/Alcohol Induced Cause Code'] != 'O9')]
mortality_data
