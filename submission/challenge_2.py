# This script completes Challenge #2 for the City of Cape Town - Data Science Unit Code Challenge
# https://github.com/cityofcapetown/ds_code_challenge
#
# Step 1.  Retrieves credentials from CREDENTIALS_URL  
# Step 2.  Create S3 Client for REGION with retrieved credentials
# Step 3.  Download SERVICE_REQUEST_SOURCE
# Step 4.  Anaylse SERVICE_REQUEST_SOURCE dataframe
# Step 5.  Determine H3 resolution level 8 hexagon for each service request
# Step 6.  Insert h3_level8_index to dataframe
# Step 7.  Download SERVICE_REQUEST_HEX_SOURCE
# Step 8.  Validate against SERVICE_REQUEST_HEX_SOURCE dataframe and save output

from support_library import(set_s3_client, 
                            BUCKET_NAME, 
                            download_file_from_s3_client,
                            delete_file,
                            SERVICE_REQUEST_SOURCE,
                            SERVICE_REQUEST_HEX_SOURCE,
                            SERVICE_REQUEST_HEX_COLUMN_NAME,
                            CHALLENGE_2_OUTPUT,
                            CHALLENGE_2_LOG,
                            ERROR_THRESHOLD,
                            )

from loguru import logger
import timeit
import os
import sys

import gzip
import pandas as pd
import h3

def calculate_h3_level8_index(x):
# Function is applied to each row in the dataframe using .apply()
# The last column         x[-1] is 'longitude'
# The second last column  x[-2] is 'latitude' 

   if not x[-2] or not x[-1]:
     return 0
   else:
     # h3.geo_to_h3(lat, lon, resolution) returns the h3_level8_index for the lat/lon coordinates
     return h3.geo_to_h3(x[-2], x[-1], 8)
 
def main():
    # -------------------------------------------------------------------------
    # Step 1.  Retrieves credentials from CREDENTIALS_URL  
    # Step 2.  Create S3 Client for REGION with retrieved credentials
    s3_client = set_s3_client()

    # -------------------------------------------------------------------------
    # Step 3.  Download SERVICE_REQUEST_SOURCE
    if os.path.exists(SERVICE_REQUEST_SOURCE):
        # this will use cached files to save time required to download.
        logger.info(f"Service data file found: '{SERVICE_REQUEST_SOURCE}'")
        is_service_data_downloded = True
    else:
        # this will download SERVICE_REQUEST_SOURCE
        process_start_time = timeit.default_timer()
        is_service_data_downloded = download_file_from_s3_client(
          s3_client, 
          BUCKET_NAME, 
          SERVICE_REQUEST_SOURCE
          )
        time_elapsed = timeit.default_timer() - process_start_time      
        if is_service_data_downloded:
          logger.info(f"Service data file downloaded: '{SERVICE_REQUEST_SOURCE}'. Time Taken: {time_elapsed}s")
        else:
          logger.error(f"Service data file download failed: '{SERVICE_REQUEST_SOURCE}'")

    # -------------------------------------------------------------------------
    # Step 4.  Anaylse SERVICE_REQUEST_SOURCE
    error_threshold_exceeded = True
    try:
        with gzip.open(SERVICE_REQUEST_SOURCE) as f_:
            service_requests = pd.read_csv(f_)
            
        # analyse dataframe for errors
        num_requests = len(service_requests)
        # invalid data (null) in 'latitude' or 'longitude'
        num_lat_errors  = service_requests['latitude'].isnull().sum()
        num_lon_errors  = service_requests['longitude'].isnull().sum()
        # num_diff_errors==0 indicate that when 'latitude', 'longitude' is also null
        num_diff_errors =  len(service_requests['latitude'].isnull().compare(service_requests['longitude'].isnull()))
        total_errors = max(num_lat_errors,num_lon_errors) + num_diff_errors
        
        logger.info(f"Number of service requests: {num_requests}")
        logger.info(f"Number of service requests with invalid 'latitude': {num_lat_errors}")
        logger.info(f"Number of service requests with invalid 'longitude': {num_lon_errors}")
        logger.info(f"Number of service requests with invalid 'latitude' or 'longitude': {total_errors}")

        if total_errors/num_requests > ERROR_THRESHOLD:
            logger.error(f"The error percentage is too high: '{total_errors/num_requests}'")
            error_threshold_exceeded = True
        else:
            error_threshold_exceeded = False
   
    except FileNotFoundError:
        logger.exception(f"Cannot open: '{SERVICE_REQUEST_SOURCE}'")
        
    # -------------------------------------------------------------------------
    # Step 5. Determine H3 resolution level 8 hexagon for each service request
    if error_threshold_exceeded==False:
        # processing one row at a time takes very long  
        test_single_row_processing = False
        if test_single_row_processing:
            process_start_time = timeit.default_timer()
            error_count = 0
            processed_count = 0
            for x in range(0,1000):
              if service_requests.loc[[x]]["latitude"].isna()[x] or service_requests.loc[[x]]["longitude"].isna()[x]:
                error_count = error_count + 1
              else:
                h3.geo_to_h3(service_requests.loc[[x]]["latitude"][x], 
                             service_requests.loc[[x]]["longitude"][x], 8)
                processed_count = processed_count +  1
            time_elapsed = timeit.default_timer() - process_start_time
            logger.info(f"Invalid service requests = {error_count}.")
            logger.info(f"Processed {processed_count} service requests. Time Taken: {time_elapsed}s")
            logger.info(f"Estimated Required Time = {num_requests/1000*time_elapsed}s")

        # rather use the panda apply() function to each row
        process_start_time = timeit.default_timer()
        h3_level8_index = service_requests.apply(calculate_h3_level8_index, axis=1)
        time_elapsed = timeit.default_timer() - process_start_time
        logger.info(f"Computed H3 index for each service request. Time Taken: {time_elapsed}s")

        # -------------------------------------------------------------------------
        # Step 6. Insert h3_level8_index to dataframe
        service_requests[SERVICE_REQUEST_HEX_COLUMN_NAME] = h3_level8_index
        # remove the first column to match SERVICE_REQUEST_HEX_SOURCE
        service_requests = service_requests.iloc[:, 1:]
    
        # -------------------------------------------------------------------------
        # Step 7.  Download SERVICE_REQUEST_HEX_SOURCE
        if os.path.exists(SERVICE_REQUEST_HEX_SOURCE):
            logger.info(f"Validation data file found: '{SERVICE_REQUEST_HEX_SOURCE}'")
            is_service_data_downloded = True
        else:
            process_start_time = timeit.default_timer()
            is_service_data_downloded = download_file_from_s3_client(
              s3_client, 
              BUCKET_NAME, 
              SERVICE_REQUEST_HEX_SOURCE
              )
            time_elapsed = timeit.default_timer() - process_start_time
            if is_service_data_downloded:
              logger.info(f"Validation data file downloaded: '{SERVICE_REQUEST_HEX_SOURCE}'. Time Taken: {time_elapsed}s")
            else:
              logger.error(f"Validation data file download failed: '{SERVICE_REQUEST_HEX_SOURCE}'")

        # -------------------------------------------------------------------------
        # Step 8.   Validate against SERVICE_REQUEST_HEX_SOURCE and save output
        try:
            with gzip.open(SERVICE_REQUEST_HEX_SOURCE) as f_:
                valid_requests = pd.read_csv(f_)
          
            # compare dataframes
            process_start_time = timeit.default_timer()
            diff_dataframe = service_requests.compare(valid_requests)        
            time_elapsed = timeit.default_timer() - process_start_time
      
            if diff_dataframe.empty:
                logger.info(f"Validated computed dataframe against '{SERVICE_REQUEST_HEX_SOURCE}'. Time Taken: {time_elapsed}s")
                process_start_time = timeit.default_timer()
                service_requests.to_csv(CHALLENGE_2_OUTPUT, index=False)
                time_elapsed = timeit.default_timer() - process_start_time
                logger.info(f"Output saved to '{CHALLENGE_2_OUTPUT}'. Time Taken: {time_elapsed}s")
        
            else:
                logger.info(f"Computed is not the same as '{SERVICE_REQUEST_HEX_SOURCE}'")

        except FileNotFoundError:
            logger.exception(f"Cannot open: '{SERVICE_REQUEST_HEX_SOURCE}'")
            
if __name__ == "__main__":
    # This will delete all cached files and force all downloads
    delete_cached_files = True    # set to False if cached downloads are used.  
    is_success = True
    if delete_cached_files:
        is_success = delete_file(SERVICE_REQUEST_SOURCE)
        is_success = is_success and delete_file(SERVICE_REQUEST_HEX_SOURCE)
        is_success = is_success and delete_file(CHALLENGE_2_OUTPUT)
        logger.stop()
        is_success = is_success and delete_file(CHALLENGE_2_LOG)
      
    if is_success:  
        # Start timer
        start_time = timeit.default_timer()
    
        # Set logger
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        sys.tracebacklimit = 0
        logger.add(CHALLENGE_2_LOG, level="DEBUG", rotation="12:00")
    
        logger.info("Starting Challenge #2")
        main()
        time_elapsed = timeit.default_timer() - start_time
        logger.info(f"Challenge #2 Completed. Total Time Taken: {time_elapsed}s")     
        logger.stop()
    else:
        logger.error("Please close cached files and rerun.")