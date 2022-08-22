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
python3 challenge_1.py
```
### Summary
- Retrieves [credentials]("https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json")
- Creates S3 Client for REGION="af-south-1" with retrieved credentials
- Uses AWS S3 SELECT command to extract the H3 resolution 8 data from "city-hex-polygons-8-10.geojson"
- Downloads "city-hex-polygons-8-.geojson"
- Validate extracted H3 resolution 8 data against "city-hex-polygons-8-.geojson"
- Save extracted H3 resolution 8 data to city-hex-polygons-8_KN.json"

## Question 2: Initial Data Transformation
The following script attempts Challenge #2 for the City of Cape Town - Data Science Unit Code Challenge
```bash
* [challenge_2.py](https://github.com/data-engineer-za/ds_code_challenge/blob/main/submission/challenge_2.py)
```
### Summary
Output is the calculated h3_level8_index for each service request from the sr.csv.gz file, and validated against the h3_level8_index from the sr_hex.csv.gz file

## Question 5: Further Data Transformations
The following script attempts Challenge #5 for the City of Cape Town - Data Science Unit Code Challenge
```bash
* [challenge_5.py](https://github.com/data-engineer-za/ds_code_challenge/blob/main/submission/challenge_2.py)
```
### Summary
A subsample of the data is created by selecting all of the requests in sr_hex.csv.gz which are within 1 minute of the centroid of the BELLVILLE SOUTH official suburb.
Appropriate wind direction and speed data for 2020 from the Bellville South Air Quality Measurement site is extracted (bellville-south-wind_data.csv) and joined to the subsample (sr_hex_subsample_joined_KN.csv)
The final output then anonymised the subsample (sr_hex_subsample_anonymised_KN).