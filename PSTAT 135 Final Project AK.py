# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1.1

# COMMAND ----------

# MAGIC %md
# MAGIC ![Final Project Screenshot 1.png](./Final Project Screenshot 1.png "Final Project Screenshot 1.png")![Final Project Screenshot 2.png](./Final Project Screenshot 2.png "Final Project Screenshot 2.png")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bigquery_connection_catalog.voterfile;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1.2 Shell Script in Cloud Shell

# COMMAND ----------

# MAGIC %md
# MAGIC ![Final Project Screenshot 3.png](./Final Project Screenshot 3.png "Final Project Screenshot 3.png")
# MAGIC
# MAGIC This shell script defines a Bash array called states that contains the two-letter abbreviations for all 50 U.S. states and the District of Columbia (DC). The script uses echo to print a single example state (${states[1]} = AL) and the full list of state abbreviations (${states[@]}) This confirms that the array is correctly formed and can be used later to iterate through each state and run queries. The script was run in Google Cloud Shell, and the output was captured as a screenshot for submission.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3. Voter Turnout Query

# COMMAND ----------

# MAGIC %md
# MAGIC ![Final Project Screenshot 4.png](./Final Project Screenshot 4.png "Final Project Screenshot 4.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ![Final Project Screenshot 5.png](./Final Project Screenshot 5.png "Final Project Screenshot 5.png")![Final Project Screenshot 6.png](./Final Project Screenshot 6.png "Final Project Screenshot 6.png")
# MAGIC
# MAGIC The notebook is running on the BigQuery compute engine in the US region. I determined this by clicking the job status line below the query output, which revealed the compute backend and region. On the “Open Job” page, I learned more about the execution details such as how many bytes were processed, whether results were cached, and the time taken. This information helps assess the performance and cost of queries.

# COMMAND ----------

# MAGIC %md
# MAGIC In Step 1.2, using Cloud Shell with a bash script was fast, scriptable, and ideal for automating repetitive tasks like querying all 50 state tables. It’s great for batch jobs or headless processing. However, it lacked visualization and was harder to debug or explore interactively.
# MAGIC
# MAGIC In Step 1.3, using a BigQuery Notebook allowed for rich interactivity, seamless integration of SQL and Python, and immediate data visualization using bigframes. It made it easy to explore, transform, and plot data. The con is that it depends on runtime availability and requires more configuration to get started (e.g., project setup, permissions).
# MAGIC
# MAGIC Overall, Step 1.2 is better for scripting and automation, while Step 1.3 is better for analysis, exploration, and visualization.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parts 2.1 and 2.2

# COMMAND ----------

# MAGIC %md
# MAGIC For the state, we ended up choosing Alaska as our state for the project. We will be exploring its voter data and demographics.

# COMMAND ----------

_sqldf.write.format("parquet") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .save("/Volumes/assignment2/default/voterfile-export/state")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS state (
# MAGIC   -- define schema explicitly if needed
# MAGIC )
# MAGIC USING PARQUET;
# MAGIC
# MAGIC COPY INTO state
# MAGIC FROM '/Volumes/assignment2/default/voterfile-export/state/'
# MAGIC FILEFORMAT = PARQUET;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ak_tmp AS
# MAGIC SELECT * FROM bigquery_connection_catalog.voterfile.ak_table;
# MAGIC
# MAGIC SELECT COUNT(*) FROM ak_tmp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2.3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 1: 6 different Pyspark Pandas APIs to perform various data exploration tasks.

# COMMAND ----------

print(ak.columns)

# COMMAND ----------

import pyspark.pandas as ps

ak_spark_df = spark.table("assignment2.default.ak")

# Convert to pandas-on-Spark DataFrame
ak = ak_spark_df.pandas_api()

# head() function
ak.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC This command displays the first 5 rows of your PySpark-on-Pandas DataFrame (ak). It helps you get a sense of what the data looks like right after loading it.

# COMMAND ----------

ak.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC This command summarizes key descriptive statistics for numeric columns in the dataset. Specifically, .describe() provides the count, mean, standard deviation, minimum, 25th percentile, median (50%), 75th percentile, and maximum values for each column.

# COMMAND ----------

import pyspark.pandas as ps

ak_spark_df = spark.table("assignment2.default.ak")
ak = ak_spark_df.pandas_api()

ak.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC This command counts the number of missing (null) values in each column. It helps identify which variables have incomplete data. In the output, some columns such as PRI_BLT_2019 and PRI_BLT_2018 had 548,259 missing values, meaning they’re entirely empty and can likely be dropped. This step is useful for quickly assessing data quality and deciding how to handle missing information.

# COMMAND ----------

import pyspark.pandas as ps

ak = ps.read_table("assignment2.default.ak")

ak['Parties_Description'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC The value_counts() function was applied to the Parties_Description column to understand how political party affiliations are distributed in the dataset. The results show that a large portion of voters (237,742) have their party listed as "Unknown," which may indicate missing or unclassified data. The most represented explicit affiliations are "Republican" with 133,370 voters and "Democratic" with 73,994. Additionally, 75,520 voters are labeled as "Non-Partisan," suggesting no declared party preference. Smaller groups include voters affiliated with "Independence," "Libertarian," "Other," "Green Libertarian," and "Constitution" parties. This distribution provides useful insight into the political composition and data completeness within the voter records.

# COMMAND ----------

import pyspark.pandas as ps

ak_spark_df = spark.table("assignment2.default.ak")

ak = ak_spark_df.pandas_api()

ak.info()

# COMMAND ----------

# MAGIC %md
# MAGIC The .info() method provides a concise summary of the dataset. It reports the number of entries (i.e., rows), the names and data types of each column, and how many non-null (i.e., non-missing) values exist in each column. This is useful for quickly identifying potential issues with missing data and understanding the structure of the DataFrame.
# MAGIC
# MAGIC For example, if the ak.info() output tells us that the dataset has 548,259 rows and 15 columns, and that all columns have 548,259 non-null entries, we can conclude that the dataset is complete with no missing values. If some columns have significantly fewer non-null values, that could indicate incomplete data or optional fields. Additionally, the data types (e.g., int64, float64, object/string) help us verify whether the columns are interpreted correctly, which is especially important before performing calculations or visualizations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 2: Voter Turnout by Age Groups 

# COMMAND ----------

import pyspark.pandas as ps
#ak = ps.read_table("assignment2.default.ak")

# Sorting Voters in to age groups
def age_to_group(age):
    if age < 30:
        return '20s'
    elif age < 40:
        return '30s'
    elif age < 50:
        return '40s'
    elif age < 60:
        return '50s'
    elif age < 70:
        return '60s'
    elif age < 80:
        return '70s'
    elif age < 90:
        return '80s'
    else:
        return '90+'

# Creating an age group column
ak['age_group'] = ak['Voters_Age'].apply(age_to_group)

# COMMAND ----------

import pyspark.pandas as ps
import matplotlib.pyplot as plt
import plotly.express as px

# Primary election columns
primary_cols = [col for col in ak.columns if col.startswith("PRI") and "GEN" not in col]
primary_cols.append("age_group")

# Long format
ak_subset = ak[primary_cols]
ak_long = ak_subset.melt(id_vars='age_group', var_name='year', value_name='vote_value')

ak_long['year'] = ak_long['year'].apply(lambda x: int(''.join(filter(str.isdigit, x))[:4]))

ak_long['voted'] = ak_long['vote_value'] == 'R'

# Group and count
grouped = ak_long[ak_long['voted']].groupby(['year', 'age_group']).size().reset_index(name='count')

pivoted = grouped.pivot(index='year', columns='age_group', values='count').fillna(0).sort_index()

# Melt
df_plot = pivoted.reset_index().melt(id_vars='year', var_name='Age Group', value_name='Voter Count')

df_plot_pd = df_plot.to_pandas()

fig = px.line(
    df_plot_pd,
    x='year',
    y='Voter Count',
    color='Age Group',
    markers=True,
    title='Voter Turnout by Age Group (Primary Elections) in Alaska'
)
fig.update_layout(xaxis_title='Year', yaxis_title='Number of Voters', template='plotly_white')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The line chart titled "Voter Turnout by Age Group (Primary Elections)" illustrates how voter turnout in Alaska's primary elections has changed over time for various age groups between 2004 and 2020. The x-axis represents election years (every even year), while the y-axis shows the number of voters. Each line corresponds to a different age group, ranging from individuals in their 20s to those aged 90 and above.
# MAGIC
# MAGIC The chart reveals that individuals in their 50s consistently had the highest turnout, peaking at over 12,000 voters around 2014. This group is followed closely by those in their 60s, who also show strong participation levels across all years. In contrast, voters in their 20s and 30s have the lowest turnout, reflecting common trends in civic engagement where younger populations tend to vote less frequently.
# MAGIC
# MAGIC Most age groups experienced noticeable spikes in turnout during 2014 and 2020, potentially due to more competitive or high-profile elections in those years. Interestingly, there was a dip in turnout around 2016, which may indicate lower engagement during that primary season. Overall, the data suggests a gradual upward trend in voter turnout over the years, especially among older age groups.
# MAGIC
# MAGIC This visualization highlights the importance of targeting voter outreach and education efforts toward younger demographics, who consistently underperform in turnout compared to older adults.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 3: Choropleth Visualization of Voter Turnout Rate by Alaska County for 2020

# COMMAND ----------

import pandas as pd
import plotly.express as px
import requests

# Step 1: Load your subset (already filtered in Spark)
ak = spark.read.table("assignment2.default.ak").select("County", "PRI_BLT_2020").toPandas()
ak = ak[ak["PRI_BLT_2020"].isin(["R", "D"])]
ak["County"] = ak["County"].str.upper().str.strip()

# Step 2: Group total ballots by county
turnout = ak.groupby("County").size().reset_index(name="total_ballots")

# Step 3: FIPS mapping for Alaska
county_fips_map = {
    "ALEUTIANS EAST": "02013", "ALEUTIANS WEST": "02016", "ANCHORAGE": "02020", "BETHEL": "02050",
    "BRISTOL BAY": "02060", "DENALI": "02068", "DILLINGHAM": "02070", "FAIRBANKS NORTH STAR": "02090",
    "HAINES": "02100", "HOONAH-ANGOON": "02105", "JUNEAU": "02110", "KENAI PENINSULA": "02122",
    "KETCHIKAN GATEWAY": "02130", "KODIAK ISLAND": "02150", "KUSILVAK": "02158", "LAKE AND PENINSULA": "02164",
    "MATANUSKA-SUSITNA": "02170", "NOME": "02180", "NORTH SLOPE": "02185", "NORTHWEST ARCTIC": "02188",
    "PETERSBURG": "02195", "PRINCE OF WALES-HYDER": "02198", "SITKA": "02220", "SKAGWAY": "02230",
    "SOUTHEAST FAIRBANKS": "02240", "VALDEZ-CORDOVA": "02261", "WRANGELL": "02275", "YAKUTAT": "02282",
    "YUKON-KOYUKUK": "02290"
}
turnout["fips"] = turnout["County"].map(county_fips_map)
turnout = turnout.dropna(subset=["fips"])

# Step 4: Download full US counties GeoJSON and filter Alaska
url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
geojson_all = requests.get(url).json()
alaska_geojson = {
    "type": "FeatureCollection",
    "features": [f for f in geojson_all["features"] if f["id"].startswith("02")]
}

# Step 5: Plot Alaska-only choropleth
fig = px.choropleth(
    turnout,
    geojson=alaska_geojson,
    locations="fips",
    color="total_ballots",
    color_continuous_scale="Blues",
    labels={"total_ballots": "Ballots"},
    title="2020 Alaska Primary Voter Turnout by County"
)

# Add borders to distinguish white counties from background
fig.update_traces(marker_line_width=1, marker_line_color='black')

# Zoom to Alaska and style map
fig.update_geos(
    visible=False,
    projection_type="mercator",
    projection_scale=6.5,
    bgcolor="lightgray",
    center={"lat": 63.5, "lon": -155}
)

fig.update_layout(
    height=650,
    width=900,
    margin={"r":0, "t":50, "l":0, "b":0},
    #paper_bgcolor="lightgray",
    plot_bgcolor="lightgray"
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The choropleth map displays the 2020 Alaska Primary voter turnout by county, specifically summing Republican and Democratic ballots. Each county is shaded on a blue gradient, with darker shades representing higher total ballots cast. Anchorage stands out with the darkest color, indicating the highest turnout, while several rural counties are much lighter, reflecting lower participation. The map includes a clear color scale legend and a gray background to improve visual contrast, making county boundaries more distinct. Alaska is reprojected and enlarged for better visibility, allowing for a clearer interpretation of regional turnout differences across the state.

# COMMAND ----------

import pyspark.pandas as ps
import matplotlib.pyplot as plt

ak = ps.read_table("assignment2.default.ak")

# Filter for R and D(Republicans and Democrats)
ak_voted = ak[ak['PRI_BLT_2020'].isin(['R', 'D'])]

# Grouping by county and count
turnout_by_county = ak_voted.groupby('County').size().sort_values(ascending=False).to_pandas()

plt.figure(figsize=(12, 6))
turnout_by_county.plot(kind='bar', color='skyblue')
plt.title('2020 Alaska Primary - Total Ballots by County')
plt.xlabel('County')
plt.ylabel('Total Ballots')
plt.xticks(rotation=60)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The bar chart presents the total number of ballots cast (Republican + Democratic) by county in the 2020 Alaska Primary. Anchorage clearly leads with the highest turnout, exceeding 30,000 ballots, which is significantly greater than any other county. Matanuska-Susitna and Fairbanks North Star follow but with substantially lower totals. The majority of Alaska’s counties display relatively low voter participation, with many registering fewer than 2,000 ballots. This chart highlights the stark urban–rural divide in voter turnout, where a few populous counties dominate the state's electoral participation, while rural and remote regions contribute much smaller numbers. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 4: Choropleth Visualizations of Party Affiliation by County

# COMMAND ----------

import pandas as pd
import plotly.express as px
import requests

ak_spark = spark.read.table("assignment2.default.ak")
ak = ak_spark.select("County", "PRI_BLT_2020").toPandas()

ak = ak[ak["PRI_BLT_2020"].isin(["R", "D"])].copy()
ak["County"] = ak["County"].str.upper().str.strip()

# R and D voters per county
party_counts = ak.groupby(["County", "PRI_BLT_2020"]).size().unstack(fill_value=0)
party_counts["Total"] = party_counts["R"] + party_counts["D"]
party_counts["Pct_Rep"] = party_counts["R"] / party_counts["Total"] * 100
party_counts.reset_index(inplace=True)

# FIPS Codes
county_fips_map = {
    "ALEUTIANS EAST": "02013", "ALEUTIANS WEST": "02016", "ANCHORAGE": "02020", "BETHEL": "02050",
    "BRISTOL BAY": "02060", "DENALI": "02068", "DILLINGHAM": "02070", "FAIRBANKS NORTH STAR": "02090",
    "HAINES": "02100", "HOONAH-ANGOON": "02105", "JUNEAU": "02110", "KENAI PENINSULA": "02122",
    "KETCHIKAN GATEWAY": "02130", "KODIAK ISLAND": "02150", "KUSILVAK": "02158", "LAKE AND PENINSULA": "02164",
    "MATANUSKA-SUSITNA": "02170", "NOME": "02180", "NORTH SLOPE": "02185", "NORTHWEST ARCTIC": "02188",
    "PETERSBURG": "02195", "PRINCE OF WALES-HYDER": "02198", "SITKA": "02220", "SKAGWAY": "02230",
    "SOUTHEAST FAIRBANKS": "02240", "VALDEZ-CORDOVA": "02261", "WRANGELL": "02275", "YAKUTAT": "02282",
    "YUKON-KOYUKUK": "02290"
}
party_counts["fips"] = party_counts["County"].map(county_fips_map)
party_counts = party_counts.dropna(subset=["fips"])

# Downloading GeoJSON and extracting only Alaska counties
url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
geojson_all = requests.get(url).json()
alaska_geojson = {
    "type": "FeatureCollection",
    "features": [f for f in geojson_all["features"] if f["id"].startswith("02")]
}

# Choropleth of Republican party
fig = px.choropleth(
    party_counts,
    geojson=alaska_geojson,
    locations="fips",
    color="Pct_Rep",
    color_continuous_scale="Reds",
    labels={"Pct_Rep": "% Republican"},
    title="2020 Alaska Primary: % Republican Voters by County"
)

fig.update_traces(marker_line_width=1, marker_line_color='black')
fig.update_geos(
    visible=False,
    projection_type="mercator",
    projection_scale=6.5,
    center={"lat": 63.5, "lon": -155}
)
fig.update_layout(
    height=650,
    width=900,
    margin={"r":0, "t":50, "l":0, "b":0},
    plot_bgcolor="lightgray"
)

fig.show()

# COMMAND ----------

import pandas as pd
import plotly.express as px
import requests

ak = spark.read.table("assignment2.default.ak").select("County", "PRI_BLT_2020").toPandas()
ak = ak[ak["PRI_BLT_2020"].isin(["R", "D"])]
ak["County"] = ak["County"].str.upper().str.strip()

# Democrat percentage by county
total = ak.groupby("County").size()
dems = ak[ak["PRI_BLT_2020"] == "D"].groupby("County").size()
pct_dem = (dems / total * 100).reset_index()
pct_dem.columns = ["County", "percent_democrat"]

# FIPS codes
county_fips_map = {
    "ALEUTIANS EAST": "02013", "ALEUTIANS WEST": "02016", "ANCHORAGE": "02020", "BETHEL": "02050",
    "BRISTOL BAY": "02060", "DENALI": "02068", "DILLINGHAM": "02070", "FAIRBANKS NORTH STAR": "02090",
    "HAINES": "02100", "HOONAH-ANGOON": "02105", "JUNEAU": "02110", "KENAI PENINSULA": "02122",
    "KETCHIKAN GATEWAY": "02130", "KODIAK ISLAND": "02150", "KUSILVAK": "02158", "LAKE AND PENINSULA": "02164",
    "MATANUSKA-SUSITNA": "02170", "NOME": "02180", "NORTH SLOPE": "02185", "NORTHWEST ARCTIC": "02188",
    "PETERSBURG": "02195", "PRINCE OF WALES-HYDER": "02198", "SITKA": "02220", "SKAGWAY": "02230",
    "SOUTHEAST FAIRBANKS": "02240", "VALDEZ-CORDOVA": "02261", "WRANGELL": "02275", "YAKUTAT": "02282",
    "YUKON-KOYUKUK": "02290"
}
pct_dem["fips"] = pct_dem["County"].map(county_fips_map)
pct_dem = pct_dem.dropna(subset=["fips"])

# Step 4: Loading Alaska counties from GeoJSON
geojson_url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
geojson_all = requests.get(geojson_url).json()
alaska_geojson = {
    "type": "FeatureCollection",
    "features": [f for f in geojson_all["features"] if f["id"].startswith("02")]
}

# Choropleth for Democrats
fig = px.choropleth(
    pct_dem,
    geojson=alaska_geojson,
    locations="fips",
    color="percent_democrat",
    color_continuous_scale="Blues",
    labels={"percent_democrat": "% Democrat"},
    title="2020 Alaska Primary: % Democrat Voters by County"
)

fig.update_traces(marker_line_width=1, marker_line_color='black')
fig.update_geos(visible=False, projection_type="mercator", projection_scale=6.5,
                center={"lat": 63.5, "lon": -155}, bgcolor="lightgray")
fig.update_layout(height=650, width=900, margin={"r":0, "t":50, "l":0, "b":0}, plot_bgcolor="lightgray")

fig.show()

# COMMAND ----------

import pandas as pd
import plotly.express as px
import requests

ak = spark.read.table("assignment2.default.ak").select("County", "PRI_BLT_2020").toPandas()
ak = ak[ak["PRI_BLT_2020"].isin(["R", "D"])]
ak["County"] = ak["County"].str.upper().str.strip()

# Vote counts by County and Party
vote_counts = ak.groupby(["County", "PRI_BLT_2020"]).size().reset_index(name="count")
pivoted = vote_counts.pivot(index="County", columns="PRI_BLT_2020", values="count").fillna(0)
pivoted["Total"] = pivoted["R"] + pivoted["D"]
pivoted["Lean"] = (pivoted["R"] - pivoted["D"]) / pivoted["Total"] * 100  # +R / -D

# FIPS codes
county_fips_map = {
    "ALEUTIANS EAST": "02013", "ALEUTIANS WEST": "02016", "ANCHORAGE": "02020", "BETHEL": "02050",
    "BRISTOL BAY": "02060", "DENALI": "02068", "DILLINGHAM": "02070", "FAIRBANKS NORTH STAR": "02090",
    "HAINES": "02100", "HOONAH-ANGOON": "02105", "JUNEAU": "02110", "KENAI PENINSULA": "02122",
    "KETCHIKAN GATEWAY": "02130", "KODIAK ISLAND": "02150", "KUSILVAK": "02158", "LAKE AND PENINSULA": "02164",
    "MATANUSKA-SUSITNA": "02170", "NOME": "02180", "NORTH SLOPE": "02185", "NORTHWEST ARCTIC": "02188",
    "PETERSBURG": "02195", "PRINCE OF WALES-HYDER": "02198", "SITKA": "02220", "SKAGWAY": "02230",
    "SOUTHEAST FAIRBANKS": "02240", "VALDEZ-CORDOVA": "02261", "WRANGELL": "02275", "YAKUTAT": "02282",
    "YUKON-KOYUKUK": "02290"
}
pivoted["fips"] = pivoted.index.map(county_fips_map)
pivoted = pivoted.dropna(subset=["fips"])

# GeoJSON and filtering to only Alaska counties
url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
geojson_all = requests.get(url).json()
alaska_geojson = {
    "type": "FeatureCollection",
    "features": [f for f in geojson_all["features"] if f["id"].startswith("02")]
}

# Partisan lean choropleth
fig = px.choropleth(
    pivoted,
    geojson=alaska_geojson,
    locations="fips",
    color="Lean",
    color_continuous_scale="RdBu",
    range_color=[-100, 100],
    labels={"Lean": "Partisan Lean (+R / -D)"},
    title="2020 Alaska Primary: Partisan Lean by County"
)

fig.update_traces(marker_line_width=1, marker_line_color='black')
fig.update_geos(
    visible=False,
    projection_type="mercator",
    projection_scale=6.5,
    bgcolor="lightgray",
    center={"lat": 63.5, "lon": -155}
)

fig.update_layout(height=650, width=900, margin={"r":0, "t":50, "l":0, "b":0}, plot_bgcolor="lightgray")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC In analyzing party affiliation across Alaska’s counties during the 2020 primary, the three choropleth visualizations show us which parts of Alaska lean towards each party. The first map, which displays the percentage of Republican voters by county, suggests that Republican affiliation was strongest in Alaska’s interior and more rural regions. Several counties reported over 80% Republican participation. In contrast, the second map, focused on Democratic affiliation, shows higher concentrations of Democratic voters in coastal and southeastern counties, particularly in urban centers such as Juneau. To synthesize these perspectives, the third map visualizes the net partisan lean by subtracting the percentage of Republican voters from Democratic voters. This difference map clearly illustrates the partisan divide across the state. Counties shaded in red leaned Republican, blue counties leaned Democrat, and white counties were more evenly split Together, these visualizations highlight the geographic polarization of political identity in Alaska, with Republicans dominant in rural and inland areas and Democrats stronger in urban and coastal regions.

# COMMAND ----------

# MAGIC %md
# MAGIC # **Part 3**

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Supervised Learning Question**

# COMMAND ----------

# MAGIC %md
# MAGIC Can we predict whether a voter will turn out to vote in the primary election based on their age, county, ethnicity, and household characteristics?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Fitting and Diagnostics

# COMMAND ----------

from pyspark.sql.functions import col
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import pandas as pd

# Step 1: Load data from Spark table
ak = spark.read.table("assignment2.default.ak")

# Step 2: Filter relevant rows (skip Voters_Active filter for now)
ak_filtered = ak.filter(
    (col("PRI_BLT_2020").isin(["R", "D", "O"])) &
    (col("Voters_Age").isNotNull()) &
    (col("Parties_Description").isNotNull())
)

# Step 3: Convert to pandas (if small enough)
ak_pd = ak_filtered.select("PRI_BLT_2020", "Voters_Age", "Parties_Description").toPandas()

# Step 4: Drop missing values (if any remain)
ak_pd = ak_pd.dropna()

# Step 5: Encode categorical variables
X = pd.get_dummies(ak_pd[["Voters_Age", "Parties_Description"]], drop_first=True)
y = ak_pd["PRI_BLT_2020"]

# Step 6: Fit logistic regression
model = LogisticRegression(max_iter=500, multi_class='multinomial', solver='lbfgs')
model.fit(X, y)

# Step 7: Output evaluation (on training set for now)
y_pred = model.predict(X)
print(classification_report(y, y_pred))

# COMMAND ----------

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler, LabelEncoder

# Removing missing values
ak_pdf_clean = ak_pdf.dropna()

# Categorical columns
ak_pdf_encoded = ak_pdf_clean.copy()
label_encoders = {}

for col in ["County", "Ethnic_Description"]:
    le = LabelEncoder()
    ak_pdf_encoded[col] = le.fit_transform(ak_pdf_encoded[col])
    label_encoders[col] = le

X = ak_pdf_encoded[["Voters_Age", "County", "Ethnic_Description", "Residence_Families_HHCount"]]
y = ak_pdf_encoded["Voted_Primary"]

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Training and test sets
X_train, X_test, y_train, y_test = train_test_split(
    X_scaled, y, test_size=0.2, random_state=42
)

# Fitting logistic regression with class weight balanced
model = LogisticRegression(class_weight='balanced', max_iter=500)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred))

# COMMAND ----------

# MAGIC %md
# MAGIC We investigated the supervised learning question: Can we predict whether a voter will turn out for the primary election based on their age, county, ethnicity, and household characteristics? Using logistic regression, we trained a binary classification model with the target variable Voted_Primary (1 for voted, 0 for not). The final model yielded an accuracy of 61% on the test set. More specifically, the model achieved a precision of 0.78 and recall of 0.59 for class 0 (non-voters), and a precision of 0.44 and recall of 0.66 for class 1 (voters).
# MAGIC
# MAGIC This output indicates that the model is better at identifying those who did not vote, but moderately effective at finding those who did. The relatively lower precision for voters (0.44) suggests that when the model predicts someone will vote, it is often incorrect — meaning campaigns may over-target certain individuals if they rely solely on the model. However, the 66% recall for voters means that it can still identify a good portion of the actual voters, which is useful in practice.
# MAGIC
# MAGIC This question and model are directly relevant to data-driven campaigning. Campaigns often operate with limited resources, so identifying likely voters or persuadable non-voters allows for targeted outreach, personalized messaging, and more efficient resource allocation. For instance, households predicted as low-likelihood voters in key counties could be prioritized for phone banking or door-to-door canvassing.
# MAGIC
# MAGIC While the model’s performance isn’t perfect, it provides a foundation for further refinement such as adding more features like voting history, registration dates, or media exposure

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Visualizations

# COMMAND ----------

from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

cm = confusion_matrix(y_test, y_pred)
disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["No Vote", "Voted"])
disp.plot(cmap='Blues', values_format='d')
plt.title("Confusion Matrix")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The confusion matrix reveals that out of 59,004 voters in the test set, the model correctly identified 23,232 individuals who did not vote and 12,805 individuals who did vote in the primary election. However, it also misclassified 16,425 non-voters as voters and 6,542 voters as non-voters. This suggests that the model performs better at predicting non-voters, but still captures a significant portion of actual voters.

# COMMAND ----------

from sklearn.metrics import roc_curve, auc, roc_auc_score

# Get probability scores for the positive class (voted = 1)
y_scores = model.predict_proba(X_test)[:, 1]

# Compute ROC curve and AUC
fpr, tpr, thresholds = roc_curve(y_test, y_scores)
roc_auc = auc(fpr, tpr)

plt.figure()
plt.plot(fpr, tpr, label=f"ROC Curve (AUC = {roc_auc:.2f})")
plt.plot([0, 1], [0, 1], linestyle="--", color="gray")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("ROC Curve")
plt.legend(loc="lower right")
plt.grid(True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The ROC curve further supports this with an Area Under the Curve (AUC) of 0.66, indicating that the model has moderate ability to distinguish between voters and non-voters. While not highly predictive, this performance shows promise and suggests that basic demographic and household characteristics do carry some predictive signal. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unsupervised Learning Question

# COMMAND ----------

# MAGIC %md
# MAGIC Can we identify distinct clusters of voters based on their age, education level, and party affiliation to uncover potential targeting strategies for future outreach?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Fitting

# COMMAND ----------

import pyspark.pandas as ps
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.cluster import KMeans
import pandas as pd

ak = ps.read_table("assignment2.default.ak")

# Non-null values in relevant columns
filtered = ak[
    ak["Voters_Age"].notnull() &
    ak["County"].notnull() &
    ak["Ethnic_Description"].notnull() &
    ak["Residence_Families_HHCount"].notnull()
]

ak_pdf = filtered[[
    "Voters_Age", "County", "Ethnic_Description", "Residence_Families_HHCount"
]].to_pandas()

le_county = LabelEncoder()
le_ethnic = LabelEncoder()

ak_pdf["County_encoded"] = le_county.fit_transform(ak_pdf["County"])
ak_pdf["Ethnic_encoded"] = le_ethnic.fit_transform(ak_pdf["Ethnic_Description"])

# Step 5: Prepare data for clustering
X = ak_pdf[["Voters_Age", "County_encoded", "Ethnic_encoded", "Residence_Families_HHCount"]].values

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Fitting KMeans clustering model
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
ak_pdf["cluster"] = kmeans.fit_predict(X_scaled)

print("Cluster counts:")
print(ak_pdf["cluster"].value_counts())

print("\nCluster centroids (scaled):")
print(pd.DataFrame(kmeans.cluster_centers_, columns=["Voters_Age", "County", "Ethnicity", "HHCount"]))

ak_pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC We explored the question: “Can we group Alaska voters based on demographic features like age, ethnicity, household size, and county?” Using KMeans clustering with k=3, we grouped the voters into three distinct clusters. The cluster sizes were 184,148 (Cluster 0), 100,430 (Cluster 1), and 46,440 (Cluster 2). The cluster centroids (in scaled units) revealed that Cluster 1 included voters with higher household counts and a strong positive encoding for ethnicity and county, while Cluster 2 had older voters on average. These clusters could help campaigns target messages more effectively: for example, tailoring outreach for younger voters in urban counties (Cluster 0) versus older or multi-family households (Cluster 2). This unsupervised approach provides insight into underlying voter segments, which can inform segmentation strategies, tailored messaging, and event planning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Visualization

# COMMAND ----------

from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

# Getting cluster assignments
cluster_labels = kmeans.labels_

# PCA to reduce to 2D
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X_scaled)

pca_df = pd.DataFrame(X_pca, columns=["PC1", "PC2"])
pca_df["cluster"] = cluster_labels

plt.figure(figsize=(8, 6))
for cluster in sorted(pca_df["cluster"].unique()):
    subset = pca_df[pca_df["cluster"] == cluster]
    plt.scatter(subset["PC1"], subset["PC2"], label=f"Cluster {cluster}", alpha=0.6)

plt.title("KMeans Clustering: PCA Visualization")
plt.xlabel("Principal Component 1")
plt.ylabel("Principal Component 2")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC We applied KMeans clustering with k = 3 on voter demographics including age, ethnicity, household count, and county. The PCA scatter plot above visualizes the clusters in two dimensions. Cluster 0 (blue) appears to represent younger voters with smaller households. Cluster 1 (orange) likely corresponds to older voters concentrated in specific counties or ethnic groups. Cluster 2 (green) shows moderate values in both dimensions. The clear separation in the PCA plot suggests meaningful groupings exist within the voter population, which could help in tailoring outreach strategies or identifying underserved voter blocks in data-driven campaigns.