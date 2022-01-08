#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


#Load datasets
athleteEvents = pd.read_csv('athlete_events.csv')
countries = pd.read_csv('countries.csv')
nocRegions = pd.read_csv('noc_regions.csv')

athleteEvents
# In[3]:


countries


# In[4]:


nocRegions


# In[5]:


#To get the dataframe we want:
#Clean dataframes
#1. Remove white spaces in the "Country" column of countries dataframe at both ends
countries['Country'] = countries['Country'].str.strip()


# In[6]:


#2. Rename the "region" column to "Country" in dataframe noc to join dataframe countries on the same column
nocRegions.rename(columns={"region": "Country"}, inplace = True)


# In[7]:


#3. Clean the "Country" column in noc to match the Country column in dataframe countries

nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='Curacao', value = 'Netherlands Antilles', regex = True)
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='Antigua', value = 'Antigua and Barbuda', regex = True)   
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='Virgin Islands, US', value = 'United States', regex = True)          
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='Virgin Islands, British', value = 'United Kingdom', regex = True)          
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='Saint Kitts', value = 'Saint Kitts & Nevis', regex = True)          
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='Trinidad', value = 'Trinidad and Tobago', regex = True)          
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='USA', value = 'United States', regex = True) 
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='UK', value = 'United Kingdom', regex = True)
nocRegions[['Country']] = nocRegions[['Country']].replace(to_replace ='Trinidad', value = 'Trinidad and Tobago', regex = True)


# In[8]:


#Also remove the white spaces in the "Country" column at both ends here (in order to join)
nocRegions['Country'] = nocRegions['Country'].str.strip()


# In[9]:


#4. Create "Total GDP" column in the dataframe country 
countries['Total GDP'] = countries['GDP ($ per capita)']*countries['Population']
countries


# In[10]:


#Join dataframes
#1. First join athleteEvents with nocRegions on NOC column

join1 = athleteEvents.merge(nocRegions, how='left', on='NOC')
join1.head()

#note: Some people might ne wondering why we have to join NOC code's data to the athletes data when we 
#have the "team" column, which represents the country of an athlete. The reason is that from 1970-2017, 
#Russia and its former Soviet Union had different names throughout this period, for example, in 1992, 
#athletes are representing the Unified Team, which we would identify it as part of Russia, which we have to 
#use its NOC code to join and categorize it as Russia.


# In[11]:


#2. Then join countries on column "Country"
join2 = join1.merge(countries, how='left', on='Country')
join2[["ID","Name","Sex","Age","Region"]]


# In[12]:


join2


# In[13]:


import numpy as np
import pandas as pd
from scipy.stats import norm  
import matplotlib.pyplot as plt
import seaborn as sns


# In[14]:


#Q1
#What is the distribution of the total medals won by different countries? 
#Is it true that countries from Europe or North America tend to win more medals?


# In[15]:


#Step 1, select the columns that we want: "Country", "Region", and Medal
Q1 = join2[["Country","Region","Season", "Medal"]]
Q1.head()


# In[16]:


#In order to count total medals and different types of medals, we create four additional columns 

Q1["Total Medals"]= Q1["Medal"]

#Use map to turn Gold, Silver, Bronze all to 1
Q1["Total Medals"]= Q1["Total Medals"].map({'Gold': 1, 'Silver': 1, 'Bronze':1})

#Then turn NaN values to 0
Q1["Total Medals"]= Q1["Total Medals"].fillna(0)

Q1


# In[17]:


#Drop the columns that do not win any medals, so we can count the total medals for each country
droppedQ1 = Q1.dropna()


# In[18]:


#Use histplot:
sns.histplot(data=droppedQ1, x="Country")


# In[19]:


#Beacuse the x-axis labels are squeezed together, we change the plot format to make it more visible
plt.figure(figsize = (30,8))
plt.xticks(rotation=80)
sns.histplot(data=droppedQ1, x="Country").set(title='Total Medals Won Across Different Countries')


# In[20]:


#Because this is the toal medals combined in summer and winter olmpics, 
#let's use hue="Season" and multiple="dodge" to sort them out

plt.figure(figsize = (30,8))
plt.xticks(rotation=80)
sns.histplot(data=droppedQ1, x="Country", hue="Season", multiple="dodge").set(title='Total Medals Won Across Different Countries')


# In[21]:


#Because there are too many countries, let's try to filter out the countries that count are < 500
grouped = droppedQ1.groupby(['Country'])[['Total Medals']].sum()
filtered = grouped[grouped['Total Medals'] >= 500]
filtered.plot(kind='bar', title='Total Medals Won For Different Countries(Over 500)', figsize=(15,5))


# In[22]:


droppedQ1


# In[23]:


droppedQ1["Region"].unique()


# In[24]:


#Find the distribution of the total medals won in different regions 
droppedQ1

#There are also some white spaces in the "Region" column, let's clean them up:
droppedQ1['Region'] = droppedQ1['Region'].str.strip()

#Rename the region names 
droppedQ1["Region"] = droppedQ1["Region"].replace({'ASIA (EX. NEAR EAST)':'ASIA', 'LATIN AMER. & CARIB':'LATIN AMERICA', 'SUB-SAHARAN AFRICA':'AFRICA', 'NORTHERN AFRICA':'AFRICA'})

plt.figure(figsize = (8,4.5))
plt.xticks(rotation=80)
sns.histplot(data=droppedQ1, x="Region").set(title='Total Medals Won by Different Regions')


# In[25]:


#Create A Mosaic Plotfor differtnet Regions with regard to whether win medal or not:


# In[26]:


#Create A Column That Shows Win Medals or Not
Q1["WinMedalsOrNot"] = Q1['Medal'].map({'Gold': True, 'Silver': True, 'Bronze':True, np.nan: False})

#There are also some white spaces in the "Region" column, let's clean them up:
Q1['Region'] = Q1['Region'].str.strip()

#Rename the region names 
Q1["Region"] = Q1["Region"].replace({'ASIA (EX. NEAR EAST)':'ASIA', 'LATIN AMER. & CARIB':'LATIN AMERICA', 'SUB-SAHARAN AFRICA':'AFRICA', 'NORTHERN AFRICA':'AFRICA'})
Q1


# In[27]:


#Create Mosaic Plot
from statsmodels.graphics.mosaicplot import mosaic
props = lambda key: {'color': 'y' if 'True' in key else 'gray'}

plt.rcParams["figure.figsize"]=(30, 15)
t = mosaic(Q1, ['Region','WinMedalsOrNot'],title='Medals For Athletes Coming From Different Regions', properties=props)


# In[28]:


#Q2. Does the stereotype that Asian athletes have less chances to win a medal hold true? 
#What about African athletes? Do athletes from Africa have a higher chance of winning medals?


# In[29]:


join2.columns


# In[30]:


#Create a column that shows if an athlete won medals or not, and another column that shows if an athlete is 
#from Asia or not.  Create a cross table, and do chi-square to check whether there is a relationship between 
#winning medals and athletes from Asia. Hence, we can see if the stereotype that Asian people are less athletic, 
#so they are less likely to win medals holds true


# In[31]:


#Remember to clean up the white spaces in 'Region' column
join2['Region'] = join2['Region'].str.strip()


# In[32]:


# create a new column based on condition from Asia or not and from Africa or not
join2['FromAsia'] = np.where(join2['Region'] == 'ASIA (EX. NEAR EAST)' , True, False)
join2['FromAfrica'] = np.where((join2['Region'] == 'NORTHERN AFRICA') | (join2['Region'] == 'SUB-SAHARAN AFRICA') , True, False)


# In[33]:


#Select the columns we want from dataframe join2
Q2 = join2[["Country","Region","Medal","FromAsia","FromAfrica"]]
#Change the NaN values in column "Medal" to "DidNotWinMedals"
Q2['Medal'] = Q2['Medal'].replace(np.nan, 'DidNotWinMedals', regex=True)
Q2


# In[34]:


#Create a mosaic plot
from statsmodels.graphics.mosaicplot import mosaic
props = lambda key: {'color': 'r' if 'True' in key else 'gray'}

plt.rcParams["figure.figsize"]=(10, 3.6)
t = mosaic(Q2, ['Medal','FromAsia'],title='Athletes From Asia', properties=props)


# In[35]:


#Create a crosstab 
ct = pd.crosstab(Q2.Medal,Q2.FromAsia)
ct


# In[36]:


#Add chi-square
from scipy.stats import chi2_contingency
chi2, p, dof, ex = chi2_contingency(ct)
print("chi2 = ", chi2)
print("p-val = ", p)
print("degree of freedom = ",dof)
print("Expected:")
pd.DataFrame(ex)


# If we both hypothsize that there is no correlation between two variables, for winning medlas with regard to athletes from Asia or not, because the p-value we gained from the analysis of chi-square is smaller than 0.05, we reject the null hypothesis and conclude that there is relationship between "Medal" and athletes from Asia.
# 
# As we can see from the expected values from the chi-square crosstab, we can see Asian athletes tend to have a lower value in all Gold, Silver, and Bronze medals. However, although the numbers appear to be a difference up to hundreds, the population data is really large, hence, the differnce is really slow. In conclusion, athletes from Asia do tend to win less medals, but the difference is very small.

# In[37]:


from statsmodels.graphics.mosaicplot import mosaic
#props = lambda key: {'color': 'r' if 'True' in key else 'gray'}

props = lambda key: {'color': 'r' if 'True' in key else 'gray'}
plt.rcParams["figure.figsize"]=(10, 3.6)
t = mosaic(Q2, ['Medal','FromAfrica'],title='Athletes From Africa', properties=props)


# From this outcome, it is not surprising that African athletes did not account for most of the medals since we are looking at all the medals in different disciplines. When taking athleticism into account, we should narrow down to the sports that require high athleticsm, such as Track & Field. (Will discuss about that later!)

# In[38]:


#Create a crosstab 
ct2 = pd.crosstab(Q2.Medal,Q2.FromAfrica)
ct2


# In[39]:


#Add chi-square
from scipy.stats import chi2_contingency
chi2, p, dof, ex = chi2_contingency(ct2)
print("chi2 = ", chi2)
print("p-val = ", p)
print("degree of freedom = ",dof)
print("Expected:")
pd.DataFrame(ex)


# Same as Asian athletes, the p-value here is smaller than 0,05, we reject the null hypothesis and conclude that there is relationship between "Medal" and athletes from Africa.
# 
# 
# As we can see from the expected values from the chi-square crosstab, we can see African athletes tend to have a lower value in all Gold, Silver, and Bronze medals. However, although the numbers appear to be a difference up to hundreds, the population data is really large, hence, the differnce is really slow. In conclusion, athletes from Africa do NOT tend to win more medals in all sports!

# In[40]:


join2["Sport"].unique()


# In[41]:


#Then what is the case in the sport of "Athletics"? Let's narrow it down!
#Select the columns we want from dataframe join2
Q2 = join2[["Country","Region","Medal","FromAsia","FromAfrica","Sport","Event" ]]
#Change the NaN values in column "Medal" to "DidNotWinMedals"
Q2['Medal'] = Q2['Medal'].replace(np.nan, 'DidNotWinMedals', regex=True)
Q2


# In[42]:


#Athletics has been contested at every Summer Olympics since the birth of the modern Olympic movement at the 1896 
#Summer Olympics. The athletics program traces its earliest roots to events used in the ancient Greek Olympics. 
#The modern program includes track and field events, road running events, and racewalking events. 

#Hence, we would narrow down the sport type to 'Athletics' 
Athletics = Q2.where(Q2['Sport']=='Athletics')
Athletics.dropna(inplace=True)


# In[43]:


#Now we gained the dataframe we want that is restricted only to "Athletics"
Athletics


# In[44]:


#Create a mosaic plot 
from statsmodels.graphics.mosaicplot import mosaic
props = lambda key: {'color': 'r' if 'True' in key else 'gray'}

plt.rcParams["figure.figsize"]=(15, 7)
t = mosaic(Athletics, ['Medal','FromAfrica'],title='Athletes From Africa In The Sport of Athletics', properties=props)


# In[45]:


#Create a crosstab 
ct2 = pd.crosstab(Athletics.Medal,Athletics.FromAfrica)
ct2


# In[46]:


#Add chi-square
from scipy.stats import chi2_contingency
chi2, p, dof, ex = chi2_contingency(ct2)
print("chi2 = ", chi2)
print("p-val = ", p)
print("degree of freedom = ",dof)
print("Expected:")
pd.DataFrame(ex)


# Here, again, with the p-valuesmaller than 0,05, we reject the null hypothesis and conclude that there is relationship between "Medal" and athletes from Africa in the sport "Athletics.

# In[47]:


#Now we find out using mosaic plot to show african athletes in the "Athletics" sports is still not precise to interpret 
#Because most of the chance of an athlete is to not win medals, so whatever athlete you are from, you are not likely to 
#win medals, so we should try to compare whether the athlete win medals or not with other atheletes from different regions
#in the sport "Athletics", just like we did in Q1 


# In[48]:


Athletics["WinMedalsOrNot"] = Athletics["Medal"].replace({'DidNotWinMedals':False,'Gold':True, 'Silver':True, 'Bronze':True}, regex=True)


#Rename the region names 
Athletics["Region"] = Athletics["Region"].replace({'ASIA (EX. NEAR EAST)':'ASIA', 'LATIN AMER. & CARIB':'LATIN AMERICA', 'SUB-SAHARAN AFRICA':'AFRICA', 'NORTHERN AFRICA':'AFRICA'})

from statsmodels.graphics.mosaicplot import mosaic
props = lambda key: {'color': 'y' if 'True' in key else 'gray'}

plt.rcParams["figure.figsize"]=(20, 8)
t = mosaic(Athletics, ['Region','WinMedalsOrNot'],title='In The Sport of Athletics', properties=props)


# In[49]:


#Find out other sports:
athleteEvents["Event"].unique()


# In[50]:


#Men's amd Women's Marathon:
Marathon = Q2.where((Q2["Event"]=="Athletics Women's Marathon") | (Q2["Event"]== "Athletics Men's Marathon"))
Marathon.dropna(inplace=True)


# In[51]:


#Create the mosiac plot
Marathon["WinMedalsOrNot"] = Marathon["Medal"].replace({'DidNotWinMedals':False,'Gold':True, 'Silver':True, 'Bronze':True}, regex=True)


#Rename the region names 
Marathon["Region"] = Marathon["Region"].replace({'ASIA (EX. NEAR EAST)':'ASIA', 'LATIN AMER. & CARIB':'LATIN AMERICA', 'SUB-SAHARAN AFRICA':'AFRICA', 'NORTHERN AFRICA':'AFRICA'})

from statsmodels.graphics.mosaicplot import mosaic
props = lambda key: {'color': 'r' if 'True' in key else 'gray'}

plt.rcParams["figure.figsize"]=(20, 8)
t = mosaic(Marathon, ['Region','WinMedalsOrNot'],title="In The Event of Men's and Women's Marathon", properties=props)


# In[52]:


#Table Tennis:
TableTennis = Q2.where((Q2["Sport"]=="Table Tennis"))
TableTennis.dropna(inplace=True)


# In[53]:


#Create the mosiac plot
TableTennis["WinMedalsOrNot"] = TableTennis["Medal"].replace({'DidNotWinMedals':False,'Gold':True, 'Silver':True, 'Bronze':True}, regex=True)


#Rename the region names 
TableTennis["Region"] = TableTennis["Region"].replace({'ASIA (EX. NEAR EAST)':'ASIA', 'LATIN AMER. & CARIB':'LATIN AMERICA', 'SUB-SAHARAN AFRICA':'AFRICA', 'NORTHERN AFRICA':'AFRICA'})

from statsmodels.graphics.mosaicplot import mosaic
props = lambda key: {'color': 'b' if 'True' in key else 'gray'}

plt.rcParams["figure.figsize"]=(15, 7)
t = mosaic(TableTennis, ['Region','WinMedalsOrNot'],title="Table Tennis", properties=props)


# In[54]:


#Q3 
#Are there more and more women athletes rising and shining in the glorious Olympics games? 
#Moreover, do countries with better economic levels tend to send more female athletes?


# In[55]:


join2.columns


# In[56]:


Q3 = join2[["Country","Region","Sex","Season", "Medal", 'GDP ($ per capita)', 'Year']]
Q3.head()


# In[57]:


#There are also some white spaces in the "Region" column, let's clean them up:
Q3['Region'] = Q3['Region'].str.strip()

#Rename the region names 
Q3["Region"] = Q3["Region"].replace({'ASIA (EX. NEAR EAST)':'ASIA', 'LATIN AMER. & CARIB':'LATIN AMERICA', 'SUB-SAHARAN AFRICA':'AFRICA', 'NORTHERN AFRICA':'AFRICA'})


# In[58]:


sns.countplot(data=Q3, x="Year", hue="Sex")


# In[59]:


#Use barplot to show how many female athletes account for the total athletes over the years
Q3['isFemaleAthlete'] = Q3['Sex'].replace({'F':1,'M':0})

sns.barplot(x='Year',y='isFemaleAthlete',data=Q3).set(title='Female Athelte Percentage Over The Years')


# In[60]:


#Do countries with better economic levels tend to send more female athletes?


# In[61]:


countriesForFemale = Q3.groupby(['Country','GDP ($ per capita)'])[['isFemaleAthlete']].sum()
countriesForFemale = countriesForFemale.reset_index()
countriesForFemale


# In[62]:


dfLast = countriesForFemale.sort_values(by=['GDP ($ per capita)'], ascending=False)
dfLast


# In[63]:


#Using regplot to plot the relationship between two variables:
ax = sns.regplot(x="isFemaleAthlete", y='GDP ($ per capita)', data=countriesForFemale)


# In[64]:


ax = sns.regplot(x="isFemaleAthlete", y='GDP ($ per capita)', data=countriesForFemale, logx=True)


# In[65]:


import statsmodels.api as sm
import statsmodels.formula.api as smf

#change the column "GDP ($ per capita)" to "GDP" to be read in smf.ols
countriesForFemale = countriesForFemale.rename(columns={'GDP ($ per capita)': 'GDP'})


model1 = smf.ols('GDP ~ isFemaleAthlete', data=countriesForFemale).fit()
model1.summary()


# Here, we want to analyze the effect of X on Y, which is the column 'isFemaleAthlete' on 'GDP'. 
#  
# And the coefficient term, 5.5078, tells us the change in Y for a unit change in X. Hence, if 'isFemaleAthlete' rises by one unit then 'GDP' rises by 5.5078.
