# https://github.com/data-engineer-za/ds_code_challenge
This repository contains code and output files developed by Kumaran Naicker for submission for ds code challenge

## Setup:
Firstly, using pip3, install all dependencies and required libraries from the requirements.txt file
run:
* pip3 install -r [requirements.txt](https://github.com/data-engineer-za/ds_code_challenge/submission/requirements.txt)

## Question 1: Data Extraction
This script attempts Challenge #1 for the City of Cape Town - Data Science Unit Code Challenge
run:
* [challenge_1.py](https://github.com/data-engineer-za/ds_code_challenge/submission/challenge_1.py)
Output is a json file with a lsit opf features matching to and validated against features in city-hex-polygons-8.geojson file

## Question 2: Initial Data Transformation
This script attempts Challenge #2 for the City of Cape Town - Data Science Unit Code Challenge
run:
* [challenge_2.py](https://github.com/data-engineer-za/ds_code_challenge/submission/challenge_2.py)
Output is the calculated h3_level8_index for each service request from the sr.csv.gz file, and validated against the h3_level8_index from the sr_hex.csv.gz file

## Question 5: Further Data Transformations
This script attempts Challenge #5 for the City of Cape Town - Data Science Unit Code Challenge
run:
* [challenge_5.py](https://github.com/data-engineer-za/ds_code_challenge/submission/challenge_5.py)
A subsample of the data is created by selecting all of the requests in sr_hex.csv.gz which are within 1 minute of the centroid of the BELLVILLE SOUTH official suburb.
Appropriate wind direction and speed data for 2020 from the Bellville South Air Quality Measurement site is extracted (bellville-south-wind_data.csv) and joined to the subsample (sr_hex_subsample_joined_KN.csv)
The final output then anonymised the subsample (sr_hex_subsample_anonymised_KN).