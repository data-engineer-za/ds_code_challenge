# Submission for Data Engineering Position
This repository contains code and setup files developed by Kumaran Naicker for submission for the Data Engineering Position.

## Installation:
### Requirements:
- The submitted solutions require python 3.9.12 or greater.
- An internet connection to access the external data sources.
- Approximately 300 MB hard drive space is required for downloading and extracting the data.

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
- Note: The Loguru library is used to create a log of the execution times for Challenge #1.
  At present an intermediate output is written to disk for debugging and testing the S3 SELECT command: "tmp.json".
  In the final production version this should be removed and speed can be improved by 50%.

### Improvements
- Question 1 does not explicitly ask for an output file of geojson format. 
  If required, the structure can be extracted from "city-hex-polygons-8-10.geojson". 
  The contents of "city-hex-polygons-8_KN.json" can be used to create "city-hex-polygons-8_KN.geojson"
- A intermediate output is written to disk for debugging and testing the S3 SELECT command: "tmp.json".
  This outputting of this file can be removed in the final production version.
- Basic error handling is including; more robust management of exceptions can be included in a production version.

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
- Note: The Loguru library is used to create a log of the execution times for Challenge #1.
  At present an intermediate ouput is written to disk for debugging and testing the S3 SELECT command: "tmp.json".
  In the final production version this should be removed and speed can be improved by 50%.

### Improvements
- Question 2 does not explicitly ask for an output file. 
  The writing of sr_hex_joined_KN.csv to disk takes 1/6th of the processing time and uses ~230 MB space.
  This can be zipped if reduction is disk usage is required.
- Better management of the records with no position coordinates can be included. 
- Validation checks all fields against  "sr_hex.csv.gz".  
  In the final production version a speed improvement can be done by validating only the last 3 columns. 
- Basic error handling is including; more robust management of exceptions can be included in a production version.

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
- Note: THE PROTECTION OF PERSONAL INFORMATION ACT, ACT No. 4 OF 2013 is commonly referred to as “POPI”. 
  The Act was signed into law in November 2013, and in April 2014 certain sections of the Act came into force.
  The purpose of Act to is protect personal information, to strike a balance between the right to privacy and the need for the free flow of, 
  and access to information, and to regulate how personal information is processed.
  As such there is a legal requirement for data needs to be anonymised to protect the customers personal information.

### Improvements
- The centroid is computed very quickly and was compared against other libraries such as Shapely and was found to be faster.
  An arcgis query was used. Unfortunately, two responses are returned and the correct response is determined by using checking that ["properties"]["OFC_SBRB_NAME"]==REQUIRED_SUBURB
- Within 1 minute is interpreted as the 1 minute latitude-longitude grid around the centroid. This is computed as within +/-1 minute in longitude and within +/-1 minute in latitude.
- Again, in the final production version a speed improvement can be done by not writing of intermediate files to disk.
- Basic error handling is including; more robust management of exceptions can be included in a production version.