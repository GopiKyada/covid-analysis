# COVID-19 Data Analysis (PySpark + Hadoop)

## Dataset
- Source: Our World in Data (OWID)  
- Download: https://covid.ourworldindata.org/data/owid-covid-data.csv

## Steps
1. Upload dataset to HDFS:
   ```bash
   hdfs dfs -mkdir /covid
   hdfs dfs -put owid-covid-data.csv /covid/
