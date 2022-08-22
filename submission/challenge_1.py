# This script completes Challenge #1 for the City of Cape Town - Data Science Unit Code Challenge
# https://github.com/cityofcapetown/ds_code_challenge
#
# Step 1.  Retrieves credentials from CREDENTIALS_URL  
# Step 2.  Create S3 Client for REGION with retrieved credentials
# Step 3.  Use AWS S3 SELECT command to extract in the H3 resolution 8 data from CITY_HEX_POLYGONS_8_10_SOURCE
# Step 4.  Download CITY_HEX_POLYGONS_8_SOURCE
# Step 5.  Validate extracted H3 resolution 8 data against CITY_HEX_POLYGONS_8_SOURCE
# Step 6.  Save extracted H3 resolution 8 data to CHALLENGE_1_OUTPUT

from support_library import(set_s3_client, 
                            download_file_from_s3_client,
                            delete_file,
                            BUCKET_NAME, 
                            CITY_HEX_POLYGONS_8_10_SOURCE,
                            CITY_HEX_POLYGONS_8_SOURCE,
                            CHALLENGE_1_TMP_OUTPUT,
                            CHALLENGE_1_OUTPUT,
                            CHALLENGE_1_LOG,
                            )

from loguru import logger
import timeit
import os
import sys

import botocore.exceptions
import json

def main():
    # Step 1.  Retrieves credentials from CREDENTIALS_URL  
    # Step 2.  Create S3 Client for REGION with retrieved credentials
    s3_client = set_s3_client()

    # Step 3.  Use AWS S3 SELECT command to extract in the H3 resolution 8 data from CITY_HEX_POLYGONS_8_10_SOURCE
    # - extract features.properties.resolution = 8
    is_data_extracted = False
    try:
        process_start_time = timeit.default_timer()   # start timer for process
        # uses SQL query to select records where features.properties.resolution == 8
        response = s3_client.select_object_content(
            Bucket=BUCKET_NAME,
            Key=CITY_HEX_POLYGONS_8_10_SOURCE,
            ExpressionType='SQL',
            Expression="SELECT * from S3Object[*].features[*] s where s.properties.resolution = 8", 
            InputSerialization={"JSON": {"Type": "DOCUMENT"}, "CompressionType": "NONE"},
            OutputSerialization={'JSON': {}},
        )
        time_elapsed = timeit.default_timer() - process_start_time    # elapsed time for process
        logger.info(f"AWS S3 SELECT command completed. Time Taken: {time_elapsed}s")
        
        # extract payload data from query response and write to a temporary file: CHALLENGE_1_TMP_OUTPUT
        process_start_time = timeit.default_timer()
        f_ = open(CHALLENGE_1_TMP_OUTPUT, "w") 
        for event in response['Payload']:
            if 'Records' in event:
                records = (event['Records']['Payload'])
                f_.write(records.decode('utf-8'))        
            elif "Stats" in event:
                stats = event["Stats"]["Details"]
                logger.debug(f"AWS S3 SELECT response statistics: {stats}")
        f_.close()
        time_elapsed = timeit.default_timer() - process_start_time
        logger.info(f"JSON response written to local disk: '{CHALLENGE_1_TMP_OUTPUT}'. Time Taken: {time_elapsed}s")
        is_data_extracted = True
        
    except botocore.exceptions.EndpointConnectionError:
        logger.exception("AWS S3 Connection Failure.")
        
    except botocore.exceptions.ClientError:
        logger.exception("S3 Client Error.")
     
    except FileNotFoundError:
        logger.exception(f"Cannot write: '{CHALLENGE_1_TMP_OUTPUT}'")
     
            
    # Step 4.  Download CITY_HEX_POLYGONS_8_SOURCE
    if os.path.exists(CITY_HEX_POLYGONS_8_SOURCE):
        # this will use cached files to save time required to download.
        logger.info(f"Validation data file found: '{CITY_HEX_POLYGONS_8_SOURCE}'")
        is_validation_downloded = True
    else:
        # this will download CITY_HEX_POLYGONS_8_SOURCE
        process_start_time = timeit.default_timer()
        is_validation_downloded = download_file_from_s3_client(
          s3_client, 
          BUCKET_NAME, 
          CITY_HEX_POLYGONS_8_SOURCE,
          )
        time_elapsed = timeit.default_timer() - process_start_time        
        if is_validation_downloded:
          logger.info(f"Validation data file downloaded: '{CITY_HEX_POLYGONS_8_SOURCE}'. Time Taken: {time_elapsed}s")
        else:
          logger.error(f"Validation data file download failed: '{CITY_HEX_POLYGONS_8_SOURCE}'")
        
    # Step 5.  Validate extracted H3 resolution 8 data against CITY_HEX_POLYGONS_8_SOURCE
    is_data_valid = False
    if is_data_extracted and is_validation_downloded:
        process_start_time = timeit.default_timer()
        extracted_features = []

        # open json query response temporary file: CHALLENGE_1_TMP_OUTPUT
        try:
            with open(CHALLENGE_1_TMP_OUTPUT, 'r') as f_:
                for line in f_:
                    json_line = json.loads(line)
                    del json_line['properties']['resolution']       #delete the resolution field
                    extracted_features.append(json_line)
            f_.close()
        
        except FileNotFoundError:
            logger.exception(f"Cannot open: '{CHALLENGE_1_TMP_OUTPUT}'")
            
            
        is_feature_invalid = False
        # open downloaded CITY_HEX_POLYGONS_8_SOURCE
        try:
            with open(CITY_HEX_POLYGONS_8_SOURCE) as f_:
                valid_hex = json.loads(f_.read())
                
                # validate each feature in extracted_features against each feature in valid_hex['features']
                for ef, vf in zip(extracted_features, valid_hex['features']):
                    if ef != vf:
                        is_feature_invalid = True
                        logger.error(f"Failed to verify: \nExtracted: {ef} \nWith: {vf}")
                f_.close()
            
        except FileNotFoundError:
            logger.exception(f"Cannot open: '{CITY_HEX_POLYGONS_8_SOURCE}'")
            
        time_elapsed = timeit.default_timer() - process_start_time    
        if is_feature_invalid==False:
           # no features are invalid
           is_data_valid = True
           logger.info(f"Extracted JSON validated. Time Taken: {time_elapsed}s")
        else:
           logger.info(f"Extracted JSON not validated. Time Taken: {time_elapsed}s")

    # Step 6.  Save extracted H3 resolution 8 data to CHALLENGE_1_OUTPUT
    if is_data_valid:
      try:
          process_start_time = timeit.default_timer()
          f_ = open(CHALLENGE_1_OUTPUT, "w") 
          for ef in extracted_features:
              f_.write(json.dumps(ef) + "\n")        
          f_.close()
          time_elapsed = timeit.default_timer() - process_start_time
          logger.info(f"Validated JSON written to local disk. Time Taken: {time_elapsed}s")
          is_data_extracted = True
          
      except botocore.exceptions.EndpointConnectionError:
          logger.exception("AWS S3 Connection Failure.")
      
      except botocore.exceptions.ClientError:
          logger.exception("S3 Client Error.")
       
      except FileNotFoundError:
          logger.exception(f"Cannot write: '{CHALLENGE_1_OUTPUT}'")  
                   
if __name__ == "__main__":
    # This will delete all cached files and force all downloads
    delete_cached_files = True    # set to False if cached downloads are used.  
    is_success = True    
    if delete_cached_files:
        is_success = delete_file(CITY_HEX_POLYGONS_8_SOURCE)
        is_success = is_success and delete_file(CHALLENGE_1_TMP_OUTPUT)
        is_success = is_success and delete_file(CHALLENGE_1_OUTPUT)
        logger.stop()
        is_success = is_success and delete_file(CHALLENGE_1_LOG)
        
    if is_success:  
        # Start timer
        start_time = timeit.default_timer()
        
        # Set logger
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        sys.tracebacklimit = 0
        logger.add(CHALLENGE_1_LOG, level="DEBUG", rotation="12:00")

        logger.info("Starting Challenge #1")
        main()
        time_elapsed = timeit.default_timer() - start_time
        logger.info(f"Challenge #1 Completed. Total Time Taken: {time_elapsed}s")
        logger.stop()
    else:
        logger.error("Please close cached files and rerun.")
      