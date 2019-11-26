import os
import pandas as pd
import matplotlib.pyplot as plt

os.chdir('C:\\Users\\Felipe\\Desktop\\Duke MIDS\\Practical Tools in Data Science\\estimating-impact-of-opioid-prescription-regulations-team-8/20_intermediate_files')
data = pd.read_parquet("merged_data_pop2010.gzip")



def pre_post_graph(state, year_of_treatment, df = data, path = 'C:\\Users\\Felipe\\Desktop\\Duke MIDS\\Practical Tools in Data Science\\estimating-impact-of-opioid-prescription-regulations-team-8\\30_results'
):

'''This functions makes the pre_post analysis graph for a state which has gone through
a policy change in the year of treatment.
This function both plots the graph and saves it as a jpg file in the folder listed in path.'''

    treated = df.loc[df.STATE == state,['YEAR','deaths_per_100k']]
    control = df.loc[df.STATE != state,['YEAR','deaths_per_100k']].groupby('YEAR', as_index = False).mean()

    fig = plt.figure()
    plt.plot(treated.YEAR, treated.deaths_per_100k, marker = 'o', color = 'red')
    plt.plot(control.YEAR, control.deaths_per_100k, marker = 'o', color = 'blue')
    plt.ylim(0,15)
    plt.axvline(x = year_of_treatment, linestyle = "dashed")
    plt.legend([state,'other states'])
    plt.xlabel('Year')
    plt.ylabel('Deaths per 100k people')
    plt.title(state)
    #plt.show()

    os.chdir(path)
    fig.savefig(state + ".jpg")
    pass

os.getcwd()
#Now lets run our function for the cases we wish to analyse
pre_post_graph('FL',2010)
pre_post_graph('WA',2012)
pre_post_graph('TX',2007)



data.describe()
