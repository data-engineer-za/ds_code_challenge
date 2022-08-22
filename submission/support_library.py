# This support library contains functions commonly used by the scripts 
# submitted for the City of Cape Town - Data Science Unit Code Challenge
# https://github.com/cityofcapetown/ds_code_challenge

from loguru import logger
import os
import requests
from requests.exceptions import HTTPError
import boto3
import botocore.exceptions

# common constants
CREDENTIALS_URL = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"
REGION          = "af-south-1"
BUCKET_NAME     = "cct-ds-code-challenge-input-data"

CITY_HEX_POLYGONS_8_10_SOURCE = "city-hex-polygons-8-10.geojson"
CITY_HEX_POLYGONS_8_SOURCE    = "city-hex-polygons-8.geojson"
CHALLENGE_1_OUTPUT            = "city-hex-polygons-8_KN.json"
CHALLENGE_1_TMP_OUTPUT        = "tmp.json"
CHALLENGE_1_LOG               = "challenge_1.log"

SERVICE_REQUEST_SOURCE                = "sr.csv.gz"
SERVICE_REQUEST_HEX_SOURCE            = "sr_hex.csv.gz"
SERVICE_REQUEST_HEX_TRUNCATED_SOURCE  = "sr_hex_truncated.csv"
SERVICE_REQUEST_HEX_COLUMN_NAME       = "h3_level8_index"
CHALLENGE_2_OUTPUT                    = "sr_hex_joined_KN.csv"
CHALLENGE_2_LOG                       = "challenge_2.log"
ERROR_THRESHOLD                       = 0.4

CHALLENGE_5_ARCGIS_URL        = "https://citymaps.capetown.gov.za/agsext1/rest/services/Theme_Based/Open_Data_Service/MapServer/75/query?where=&text=BELLVILLE+SOUTH&&featureEncoding=esriDefault&f=geojson"
REQUIRED_SUBURB               = "BELLVILLE SOUTH"
WIND_DATA_SOURCE              = "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"
WIND_DATA_OUTPUT              = "Wind_direction_and_speed_2020.ods"
CHALLENGE_5_TMP_WIND_DATA     = "bellville-south-wind_data.csv"
CHALLENGE_5_TMP_OUTPUT        = "sr_hex_subsample_joined_KN"
CHALLENGE_5_OUTPUT            = "sr_hex_subsample_anonymised_KN"
CHALLENGE_5_LOG               = "challenge_5.log"
 

def get_aws_credentials(url):
#   return access_key, secret_key credentials from ds_code_challenge_creds url
#   Uses the requests library for making HTTP requests in Python
#     reference: https://realpython.com/python-requests/
#   
#   - checks for valid html response
#   - checks response Content-Type = "application/json"
#   - extracts access_key and secret from the expected json response
#   - On exceptions returns None, None

    access_key = None
    secret_key = None
    try:
        response = requests.get(url)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        logger.exception(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logger.exception(f"Error occurred: {err}")
    else:
        # Confirm the content type is "application/json" 
        content_type = response.headers['Content-Type']
        if content_type.find("application/json") == -1:
            logger.exception(f"Invalid response type: '{content_type}'")
        else:
            try:
                # extract kets from json stucture
                access_key = response.json()["s3"]["access_key"]
                secret_key = response.json()["s3"]["secret_key"]
                logger.info(f"Keys Extracted from url: '{url}'")
            except:
                logger.exception("Error occurred")
                access_key = None
                secret_key = None
          
    return access_key, secret_key
  
def set_s3_client():
#   return s3_client with region and credentials set 

    #  retrieve aws credentials from CREDENTIALS_URL
    access_key, secret_key = get_aws_credentials(CREDENTIALS_URL)
    
    #  create s3_client for REGION with credentials
    s3_client = boto3.client(
      "s3",
      region_name=REGION,
      aws_access_key_id=access_key,
      aws_secret_access_key=secret_key)

    return s3_client
    
def download_file_from_s3_client(s3_client, BUCKET_NAME, FILE_NAME):
#   downloads file from s3 client
#   return is_downloaded==True if download succeeded
    is_downloaded = False
    try:
        s3_client.download_file(
            BUCKET_NAME, 
            FILE_NAME, 
            FILE_NAME
            )
        is_downloaded = True
    
    except botocore.exceptions.EndpointConnectionError:
        logger.exception("AWS S3 Connection Failure.")
    
    except botocore.exceptions.ClientError:
        logger.exception("S3 Client Error.")
    
    except FileNotFoundError:
        logger.exception(f"Cannot write: '{FILE_NAME}'")
        
    return is_downloaded

def delete_file(file_name):
#   deletes a file on disk. 
#   returns False if there is a Permission Error
  try:
      os.remove(file_name)
      return True
    
  except FileNotFoundError:
      logger.debug(f"File not present: '{file_name}'")  
      return True
  
  except PermissionError:
      logger.exception(f"Permission error: '{file_name}'")  
      logger.error(f"Please close file before proceeding: '{file_name}'")    
      return False
