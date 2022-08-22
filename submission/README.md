# Submission for Data Engineering Position
This repository contains code and setup files developed by Kumaran Naicker for submission for the Data Engineering Position.

## Installation:
### Requirements:
- The submitted solutions require python 3.9.12 or greater.
- An internet connection to access the external data sources.
- Approximately 300 MB hard drive space for for downloading and extracting the data.

### Setup:
- Use pip3 to install all dependencies and required libraries from the [requirements.txt](https://github.com/data-engineer-za/ds_code_challenge/blob/main/submission/requirements.txt) file
```bash
pip3 install -r requirements.txt
```

## Question 1: Data Extraction
The [challenge_1.py](https://github.com/data-engineer-za/ds_code_challenge/blob/main/submission/challenge_1.py) script attempts Challenge #1 for the City of Cape Town - Data Science Unit Code Challenge
```bash
python challenge_1.py
```
### Summary
- Retrieves [credentials](https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json)
- Creates S3 Client for REGION="af-south-1" with retrieved credentials.
- Uses AWS S3 SELECT command to extract the H3 resolution 8 data from "city-hex-polygons-8-10.geojson".
- Downloads "city-hex-polygons-8-.geojson".
- Validate extracted H3 resolution 8 data against "city-hex-polygons-8-.geojson".
- Saves output to "city-hex-polygons-8_KN.json".

## Question 2: Initial Data Transformation
The [challenge_2.py](https://github.com/data-engineer-za/ds_code_challenge/blob/main/submission/challenge_2.py) script attempts Challenge #2 for the City of Cape Town - Data Science Unit Code Challenge
```bash
python challenge_2.py
```
### Summary
- Retrieves [credentials](https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json)
- Creates S3 Client for REGION="af-south-1" with retrieved credentials.
- Downloads "sr.csv.gz" and analyses dataframe for errors.
- Determines the H3 resolution level 8 hexagon for each service request.
- Inserts h3_level8_index to dataframe.
- Downloads "sr_hex.csv.gz".
- Validates dataframe against "sr_hex.csv.gz".
- Saves output "sr_hex_joined_KN.csv".

## Question 5: Further Data Transformations
The [challenge_5.py](https://github.com/data-engineer-za/ds_code_challenge/blob/main/submission/challenge_5.py) script attempts Challenge #5 for the City of Cape Town - Data Science Unit Code Challenge
```bash
python challenge_5.py
```
### Summary
- Obtains the BELLVILLE SOUTH official suburb polygon from https://odp-cctegis.opendata.arcgis.com/datasets/cctegis::official-planning-suburbs/about
- Uses arcgis [query](https://citymaps.capetown.gov.za/agsext1/rest/services/Theme_Based/Open_Data_Service/MapServer/75/query?where=&text=BELLVILLE+SOUTH&&featureEncoding=esriDefault&f=geojson) 
- Computes the centroid for BELLVILLE SOUTH.
- Loads "sr_hex.csv.gz" and creates subsample of the data by selecting requests within 1 minute of the centroid of the BELLVILLE SOUTH suburb.
- Download and prepares wind data from "Wind_direction_and_speed_2020.ods". A extracted and prepared version is saved to "bellville-south-wind_data.csv"
- Joins the wind data from the Bellville South Air Quality Measurement site to subsample. The intermediate output "sr_hex_subsample_joined_KN.csv" is saved for review purposes.
- Anonymise subsample and saves output to "sr_hex_subsample_anonymised_KN". 