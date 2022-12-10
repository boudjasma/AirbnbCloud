# Databricks notebook source
dbutils.fs.unmount("/mnt/listairbnb")

# COMMAND ----------

storage_account_name="cloudairbnbstorage"
directory_name="listairbnb"
scope_name="AirbnbScope"
key_name="secret1"
file_name="airbnblistings.csv"
database = "default"
table_name="airbnbtable"
table_cleaned = "amsterdam_airbnb_table_cleaned"

# COMMAND ----------

dbutils.fs.mount(
  source = f"wasbs://listairbnb@{storage_account_name}.blob.core.windows.net/",
  mount_point = f"/mnt/{directory_name}",
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": 
                    dbutils.secrets.get(scope = scope_name, 
                                        key = key_name)})

# COMMAND ----------

dbutils.fs.ls("/mnt/listairbnb")

# COMMAND ----------

import pandas as pd

listings = pd.read_csv("/dbfs/mnt/listairbnb/listings.csv", parse_dates=['first_review', 'last_review', 'calendar_last_scraped', 'calendar_updated', 'host_since', 'last_scraped'])

# COMMAND ----------

# Converting price type to integer

listings["price"] = listings["price"].str.replace('[\$\,]|\.\d*', '').astype(int)

# COMMAND ----------

listings.info()

# COMMAND ----------

# Visualisation
import pandas as pd
import numpy as np
import seaborn as sns
from datetime import datetime
import matplotlib.pyplot as plt
from collections import Counter
%matplotlib inline

# COMMAND ----------

# DBTITLE 1,Distribution of Listings by Bedrooms Count
# Distribution of Listings by Bedrooms Count
bedrooms_counts = Counter(listings.bedrooms)
tdf = pd.DataFrame.from_dict(bedrooms_counts, orient = 'index').sort_values(by = 0)
tdf = (tdf.iloc[-8:, :] / 2224) * 100
tdf.sort_index(axis = 0, ascending = True, inplace = True)

# Plot percent of listings by bedroom number
ax = tdf.plot(kind = 'bar', figsize = (10, 4))
ax.set_xlabel("Number of Bedrooms")
ax.set_ylabel("Frequency")
ax.set_title('Percent of Listings by Bedrooms')
ax.legend_.remove()

plt.show()

print("Percent of 1 Bedroom Listings: %{0:.2f}".format(tdf[0][1]))

# COMMAND ----------

# DBTITLE 1,Distribution of Listings by property type
# Distribution of listing by property type 
property_types = Counter(listings.property_type)
tdf = pd.DataFrame.from_dict(property_types, orient = 'index').sort_values(by = 0)
tdf = (tdf.iloc[-10:, :] / 2224) * 100

# Sort bedroom dataframe by number
tdf.sort_index(axis = 0, ascending = True)

# Plot percent of listings by bedroom number
ax = tdf.plot(kind = 'bar', figsize = (10, 4))
ax.set_xlabel("Type of property")
ax.set_ylabel("Frequency")
ax.set_title('Percent of listing by property type')
ax.legend_.remove()

plt.show()

print("Percent of entire rental unit Listings: %{0:.2f}".format(tdf[0][3]))                

# COMMAND ----------

# DBTITLE 1,Distribution of listing by room type
plt.figure(figsize = (7, 5))
properties = listings.groupby(["room_type"])["room_type"].count()

plt.gca().axis("equal")
pie = plt.pie(properties, startangle=0, autopct='%1.0f%%', pctdistance=0.7, radius=1.2)
labels=properties.index
plt.title('Distribution of listing by room type', weight='bold', size=10)
plt.legend(pie[0],labels, bbox_to_anchor=(1,0.5), loc="center right", fontsize=10, 
           bbox_transform=plt.gcf().transFigure)
plt.subplots_adjust(left=0.0, bottom=0.1, right=0.85)

plt.show()
plt.clf()
plt.close()

# COMMAND ----------

# importing packages
import seaborn
 
df = listings.groupby(['property_type'])['price'].mean()
 
# plot the result
ax = df.plot()
ax.set_xlabel("Property type")
ax.set_ylabel("Price mean")
ax.set_title('Mean price by property type listing')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

listings.groupby(['property_type'])['price'].mean().reset_index(name='mean').sort_values(['mean'], ascending=True)

# COMMAND ----------

# DBTITLE 1,Get the dwelling type composition per neighborhood
top_10 = (listings.groupby(['neighbourhood'])['id'].count()
        .sort_values(ascending = False).head(10))
top_10

# COMMAND ----------

# Let's make a list containing all the 'top_6' Series neighborhoods.
desirable_neighborhoods = list(top_10.index)

# Here, we'll perform a group by. It will list all neighborhoods 
# and their property types available. Also, it will count the number of 
# dwellings each residence kind has.
property_types_count = listings.groupby(['neighbourhood','property_type'], as_index = False)['id'].count()
property_types_count = (property_types_count.
                        sort_values(by = 'id', ascending = False)
                       ) 
property_types_count

# COMMAND ----------

# Let's make a list containing all the 'top_6' Series neighborhoods.
desirable_neighborhoods = list(top_10.index)

# We'll  filter the DF so that it only contains residences registered 
# in the 'desirable_neighborhoods' list.
property_types_count = (property_types_count[property_types_count['neighbourhood']
                                             .isin(desirable_neighborhoods)])

# And that's it! We are set to plot the chart!
import plotly.express as px

# Unfortunately plotly has a bug for the sunburst plot 
# in which it does not recognize the 'id' column for the chart creation. 

# Hence we'll have to make a second column with the same data.
# We are going to call the new column 'Number of Dwellings'
property_types_count['Number of Dwellings'] = property_types_count['id']

# Finally plotting the sunburst chart!
figure = px.sunburst(property_types_count, values = 'Number of Dwellings', 
                     path = ['neighbourhood','property_type'], width = 600, height = 600, 
                     title = 'Dwelling Type Composition')

figure.update_traces(textinfo="label+percent parent")
figure.show()

# COMMAND ----------

## 

# COMMAND ----------

# DBTITLE 1,Compare the average prices among residences with different reputations on the app
# Our categories generated by Pandas are : Poor: (1.992, 4.0] Average: (4.0, 6.0] Good: (6.0, 8.0] Excellent: (8.0, 10.0]
categories = pd.cut(listings['review_scores_value'].dropna(), 4,
                   labels = ['Poor', 'Avergage', 'Good', 'Excellent'])

# The 'categories' variable data will become a new column in the 'airbnb' DF.
listings['Lodging Quality'] = categories
listings['Lodging Quality']

# COMMAND ----------

# We'll be filtering out the residences with no customer evaluation.
no_nan = listings[~listings['review_scores_value'].isnull()]

# With no NaN values we are ready to finish our task.
from matplotlib.pyplot import style

# Setting the 'seaborn' chart style.
style.use('seaborn')

# Grouping the dwellings accordingly to their 'Lodging Quality' reputation 
# and calculating the average price for each category.
groups = no_nan.groupby('Lodging Quality')['price'].mean()

# Defining the chart's parameters
chart = groups.plot(kind = 'bar', ylim = (80,120) , xlabel = '', yticks = [], 
                    fontsize = 12, grid = False, color = ['grey','grey', 'darkred', 'grey'])

# Configuring matplotlib to display the average price for each of the graph's bars.
for i, price in enumerate(groups):
    plt.text(-0.2 + i, price + 3, f'${price:.2f}', fontdict = {'size':12})

# Defining the graph's title
plt.title('Average Price per Dwelling Quality', fontdict = {'size':15})

# COMMAND ----------

no_nan = listings[~listings['review_scores_value'].isnull()]
hlistings_cleand = no_nan.groupby(['host_id','host_name'])['review_scores_value'].mean().reset_index(name='mean').sort_values(['mean'], ascending=False)
hlistings_cleand


# COMMAND ----------


