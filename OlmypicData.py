import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *


athletes = sqlContext.read.csv("athlete_events.csv", header=True)

#register the dataframe as a table
athletes.registerTempTable("athletes")


noc = sqlContext.read.csv("noc_regions.csv", header=True)
noc.registerTempTable("noc")

countrydata = sqlContext.read.csv("countries.csv", header=True)
#Clean up the spaces in the "Country" column of the dataframe
countrydata = countrydata.withColumn("Country", trim(col("Country")))
countrydata.registerTempTable('countrydata')

#Clean the region column in noc to match the Country column in countries.csv
#Here, because SparkSQL does not support UPDATE statements yet, aside from manually changed the dataset noc_regions.csv to match the columns, 
# I used regexp_replace to replace Spark DataFrame Column Value 


nocClean = noc.withColumn('region', regexp_replace('region', 'Curacao', 'Netherlands Antilles'))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'Antigua', 'Antigua and Barbuda'))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'Virgin Islands, US', 'United States '))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'Virgin Islands, British', 'United Kingdom '))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'Saint Kitts', 'Saint Kitts & Nevis'))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'Trinidad', 'Trinidad and Tobago'))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'USA', 'United States '))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'UK', 'United Kingdom '))
nocClean = nocClean.withColumn('region', regexp_replace('region', 'Trinidad', 'Trinidad and Tobago'))
nocClean.registerTempTable('nocClean')

#Clean up the spaces in the "region" column of the dataframe
nocClean = nocClean.withColumn("region", trim(col("region")))
nocClean.registerTempTable('nocClean')


#Create a total GDP by mutilpying GDP per capita by population:
countrydata = sqlContext.sql('''SELECT Country, `GDP ($ per capita)`, `GDP ($ per capita)`* Population as Total_GDP
                             FROM countrydata
                             ''')
countrydata.registerTempTable('countrydata')


'''join the first two datasets on the same “NOC code” column. By doing this, 
the joined dataset can have a column that contains the full name of each country.'''

'''Some people might ne wondering why we have to join NOC code's data to the athletes data when we have the "team" column, 
which represents the country of an athlete. The reason is that from 1970-2017, Russia and its former Soviet Union had different names throughout this period, 
for example, in 1992, athletes are representing the Unified Team, which we would identify it as part of Russia, which we have to use its NOC code to join and categorize it as Russia.'''

#q1 = sqlContext.sql("SELECT Name, Sex, NOC, Year, Medal FROM athletes")
#q2 = sqlContext.sql("SELECT NOC, region FROM noc")


#The table that has includes all athlete data that is joined with noc
joined = sqlContext.sql('''SELECT athletes.Name, athletes.Sex, athletes.NOC, 
                    athletes.Year, athletes.Medal, nocClean.region as country
                    FROM athletes
                    JOIN nocClean
                    ON athletes.NOC = nocClean.NOC
                    ''')



#The table that has either gold, silver, or bronze medals and the data time we want(1970-2016):
medalsjoined = sqlContext.sql('''SELECT athletes.Name, athletes.Sex, athletes.NOC, 
                    athletes.Year, athletes.Medal, nocClean.region as country
                    FROM athletes
                    JOIN nocClean
                    ON athletes.NOC = nocClean.NOC
                    WHERE athletes.Medal != 'NA'
                    AND athletes.Year >= 1970
                    AND athletes.Year <= 2016
                    ''')

medalsjoined.registerTempTable('medalsjoined')


CountryAndMedals = sqlContext.sql('''SELECT Country, sum(cast((Medal == 'Gold') as int)) as Gold,
                             sum(cast((Medal == 'Silver') as int)) as Silver,
                             sum(cast((Medal == 'Bronze') as int)) as Bronze, 
                             count(*) as medals
                             FROM medalsjoined
                             GROUP BY country
                             ''')

CountryAndMedals.registerTempTable('CountryAndMedals')


#When trying to join CountryAndMedals with coutry on the same "country "column , 
#I have found out that these names: 'Trinidad', 'Curacao', 'Antigua', 'Virgin Islands, US', 'Virgin Islands, British', 'Saint Kitts', 'Trinidad' are 
#different from the countries.csv. In order to match these, I used 'set' to clean these data for matching (Have already done this in earlier steps)



# Which counties have the most medals in total? Does it have anything to do with its GDP?
CountryMedalsGDP = sqlContext.sql('''SELECT CountryAndMedals.Country, CountryAndMedals.Gold, 
                                CountryAndMedals.Silver, CountryAndMedals.Bronze, CountryAndMedals.medals,
                                countrydata.`GDP ($ per capita)` , countrydata.Total_GDP as `GDP ($ total)`
                                FROM CountryAndMedals
                                JOIN countrydata
                                ON CountryAndMedals.Country = countrydata.Country
                                ORDER BY Gold DESC, medals DESC, `GDP ($ per capita)` DESC, `GDP ($ total)` DESC
                                ''')

CountryMedalsGDP.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('CountryMedalsGDP_output')

#hadoop fs -getmerge CountryMedalsGDP_output CountryMedalsGDP_output.tsv







#Do wealthier countries tend to send larger delegations? 

#Create a delegation dataframe
delegations = sqlContext.sql('''SELECT country, 
                             count(*) as total_athletes,
                             count(DISTINCT year) as years_participated,
                             count(*)/count(DISTINCT year) as avg_delegation
                             FROM medalsjoined
                             GROUP BY country
                             ORDER BY avg_delegation DESC
                             ''')

delegations.registerTempTable("delegations")

delegationsandGDP = sqlContext.sql('''SELECT delegations.Country, delegations.avg_delegation, 
                                countrydata.`GDP ($ per capita)` , 
                                countrydata.Total_GDP as `GDP ($ total)`
                                FROM delegations
                                JOIN countrydata
                                ON delegations.country = countrydata.Country
                                ORDER BY avg_delegation DESC, `GDP ($ per capita)` DESC, `GDP ($ total)` DESC
                                ''')

delegationsandGDP.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('delegationsandGDP_output')   
#hadoop fs -getmerge delegationsandGDP_output delegationsandGDP_output.tsv


#do larger delegations increase the chances of winning medals? 

delegationsandMedals = sqlContext.sql('''SELECT delegations.Country, delegations.avg_delegation, 
                                CountryAndMedals.Gold, CountryAndMedals.Silver, CountryAndMedals.Bronze, CountryAndMedals.medals
                                FROM delegations
                                JOIN CountryAndMedals
                                ON delegations.country = CountryAndMedals.Country
                                ORDER BY avg_delegation DESC, Gold DESC, medals DESC
                                ''')
delegationsandMedals.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('delegationsandMedals_output') 
#hadoop fs -getmerge delegationsandMedals_output delegationsandMedals_output.tsv

