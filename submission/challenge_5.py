# This script completes Challenge #5 for the City of Cape Town - Data Science Unit Code Challenge
# https://github.com/cityofcapetown/ds_code_challenge
#
# Step 1.  Compute the centroid for belville south
# Step 2.  Load CHALLENGE_2_OUTPUT: sr_hex joined with the H3 Level 8 indice
# Step 3.  Create subsample of the CHALLENGE_2_OUTPUT by selecting requests  
#          within 1 minute of the centroid of the BELLVILLE SOUTH suburb
# Step 4.  Download and prepare wind data from WIND_DATA_SOURCE
# Step 5.  Join Wind Data from the Bellville South Air Quality Measurement site 
# Step 6.  Anonymise dataframe and save dataframe to disk

from support_library import(delete_file,
                            REQUIRED_SUBURB,
                            CHALLENGE_2_OUTPUT,
                            CHALLENGE_5_ARCGIS_URL,
                            WIND_DATA_SOURCE,
                            WIND_DATA_OUTPUT,
                            CHALLENGE_5_TMP_WIND_DATA,
                            CHALLENGE_5_TMP_OUTPUT,
                            CHALLENGE_5_OUTPUT, 
                            CHALLENGE_5_LOG,
                            )

from loguru import logger
import timeit

import os
import sys
import requests
from requests.exceptions import HTTPError
import matplotlib.pyplot as plt
import numpy as np 
import pandas as pd
from pandas_ods_reader import read_ods
from datetime import timedelta
import pyproj

  
def compute_belville_south_centroid():
# Function is applied to each row in the dataframe using .apply()
# All Suburbs are depicted with polygons on the City of Cape Town Corporate GIS Server
# - https://odp-cctegis.opendata.arcgis.com/datasets/cctegis::official-planning-suburbs/about
# An arcgis query was made to search fo the polygon for "BELLVILLE SOUTH"
# The urls for the query is CHALLENGE_5_ARCGIS_URL

    centroid = np.array([0.0, 0.0])
    # The CHALLENGE_5_ARCGIS_URL url returns a json response for the query with text=BELLVILLE+SOUTH
    try:
        process_start_time = timeit.default_timer()
        json_response = requests.get(CHALLENGE_5_ARCGIS_URL).json()
    
        # features for two suburbs are returned: BELLVILLE SOUTH INDUSTRIA and BELLVILLE SOUTH
        # check OFC_SBRB_NAME and extract the coordinates for REQUIRED_SUBURB = "BELLVILLE SOUTH"
        polygon_coords = []
        for suburb in json_response["features"]:
            if suburb["properties"]["OFC_SBRB_NAME"]==REQUIRED_SUBURB:
                polygon_coords = suburb["geometry"]["coordinates"][0]
        
        time_elapsed = timeit.default_timer() - process_start_time 
        logger.info(f"'{REQUIRED_SUBURB}' polygon downloaded and extracted. Time Taken: {time_elapsed}s")
        
        # compute the centroid of the coordinate
        process_start_time = timeit.default_timer()
        a = np.array(polygon_coords)
        centroid = a.sum(axis=0)/len(a)
        time_elapsed = timeit.default_timer() - process_start_time
        logger.info(f"'{REQUIRED_SUBURB}' centroid computed: {centroid[0], centroid[1]}. Time Taken: {time_elapsed}s")
        
        # this debug plot shows the shape of REQUIRED_SUBURB ande location of 
        # the centroid
        enable_debug_plot = False
        if enable_debug_plot:
            plt.rcParams.update({'font.family':'monospace'})
            plt.rcParams.update({'font.monospace':'Courier New'})
          
            hFig = plt.figure(figsize=(10, 10), dpi=100)
            for i in polygon_coords:
                plt.plot(i[0], i[1], 'r.', markersize=8)
          
            plt.plot(centroid[0], centroid[1], 'kx', markersize=8)
          # plt.plot(shapely_centroid.x, shapely_centroid.y, 'ko', markersize=8)
            ax = hFig.get_axes()
            ax[0].set_title(REQUIRED_SUBURB, fontsize=32)
            ax[0].set_ylabel('Latitude [Deg]', fontsize=24)
            ax[0].set_xlabel('Longitude [Deg]', fontsize=24)  
            for label in ax[0].get_yticklabels():
                 label.set_fontsize(14)
            for label in ax[0].get_xticklabels():
                 label.set_fontsize(14)
               
    except HTTPError as http_err:
        logger.exception(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logger.exception(f"Error occurred: {err}")

    return centroid
 
def check_if_within_1_minute_of_centroid(x, centroid_longitude, centroid_latitude):
# Function is applied to each row in the dataframe using .apply()
# The last column        x[-1] is 'h3_level8_index'
# The second last column x[-2] is 'longitude'
# The third last column  x[-3] is 'latitude'
# Returns True if coordinate is within 1 minute to the centroid
    if np.isnan(x[-3]) or np.isnan(x[-2]):
        return False
    else:
        # within 1 minute is interpreted as the 1 minute lat-long grid 
        # around the centroid.
        # This is within +/-1 minute in long and within +/-1 minute in lat
        is_long_within_1_min = abs(centroid_longitude - x[-2]) <= 1/60
        is_lat_within_1_min  = abs(centroid_latitude - x[-3]) <= 1/60
        return is_long_within_1_min and is_lat_within_1_min
 
def extract_belville_wind_data():
    is_wind_data_downloded = False
    wind_speed_df = pd.DataFrame([])
    
    if os.path.exists(WIND_DATA_OUTPUT):
        # this will use cached files to save time required to download.
        logger.info(f"Wind data file found: '{WIND_DATA_OUTPUT}'")
        is_wind_data_downloded = True
    else:
        try:
            # this will download and save WIND_DATA_OUTPUT
            process_start_time = timeit.default_timer()
            response = requests.get(WIND_DATA_SOURCE, allow_redirects=True)
      
            with open(WIND_DATA_OUTPUT, "wb") as f_download:
                f_download.write(response.content)
            f_download.close()
        
            time_elapsed = timeit.default_timer() - process_start_time      
            logger.info(f"Wind data file downloaded and saved: '{WIND_DATA_OUTPUT}'. Time Taken: {time_elapsed}s")
            is_wind_data_downloded = True
            
        except HTTPError as http_err:
            logger.exception(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.exception(f"Error occurred: {err}")
        except FileNotFoundError:
            logger.exception(f"Cannot read/write: '{WIND_DATA_OUTPUT}'")
            
        
    if is_wind_data_downloded:
        try:
            process_start_time = timeit.default_timer()
            wind_speed_df = read_ods(WIND_DATA_OUTPUT)
            time_elapsed = timeit.default_timer() - process_start_time      
            logger.info(f"Wind data file read from file: '{WIND_DATA_OUTPUT}'. Time Taken: {time_elapsed}s")
  
            # delete header rows and footer rows, leaving only the with wind data
            process_start_time = timeit.default_timer()
            wind_speed_df = wind_speed_df[4:-8]
          
            # delete columns for other stations
            wind_speed_df.pop('unnamed.1') 
            wind_speed_df.pop('unnamed.2') 
            wind_speed_df.pop('unnamed.5') 
            wind_speed_df.pop('unnamed.6') 
            wind_speed_df.pop('unnamed.7') 
            wind_speed_df.pop('unnamed.8') 
            wind_speed_df.pop('unnamed.9') 
            wind_speed_df.pop('unnamed.10') 
            wind_speed_df.pop('unnamed.11') 
            wind_speed_df.pop('unnamed.12') 
            wind_speed_df.pop('unnamed.13') 
            wind_speed_df.pop('unnamed.14') 
          
            # rename columns
            wind_speed_df = wind_speed_df.rename(columns = {
              'MultiStation:  Periodically: 01/01/2020 00:00-31/12/2020 23:59  Type: AVG 1 Hr.':'date_and_time',
              'unnamed.3':'wind_direction_deg',
              'unnamed.4':'wind_speed_m_s',
              })
           
            wind_speed_df['wind_direction_deg'][wind_speed_df['wind_direction_deg']=="<Samp"] = None
            wind_speed_df['wind_direction_deg'][wind_speed_df['wind_direction_deg']=="NoData"] = None
            wind_speed_df['wind_speed_m_s'][wind_speed_df['wind_speed_m_s']=="<Samp"] = None
            wind_speed_df['wind_speed_m_s'][wind_speed_df['wind_speed_m_s']=="NoData"] = None
            wind_speed_df['wind_speed_m_s'][wind_speed_df['wind_speed_m_s']=="Calm"] = None
            wind_speed_df['date_and_time']  = wind_speed_df['date_and_time'] + "+02:00"
            wind_speed_df.to_csv(CHALLENGE_5_TMP_WIND_DATA, index=False)
            time_elapsed = timeit.default_timer() - process_start_time      
            logger.info(f"Wind data cleaned and saved to file: '{CHALLENGE_5_TMP_WIND_DATA}'. Time Taken: {time_elapsed}s")
  
        except FileNotFoundError:
            logger.exception(f"Cannot read/write: '{WIND_DATA_OUTPUT}'")
        except PermissionError:
            logger.exception(f"Permission error: '{WIND_DATA_OUTPUT}'") 

    return wind_speed_df
  
  
def temporal_offset(Hours=6):
# create a random numnber between -Hours and +Hours
    return float(np.random.rand(1)*Hours*2 - Hours)
  

def anonomise_timestamp(x):
# Function is applied to each row in the dataframe using .apply()
# The first column         x[0] is 'creation_timestamp' 
# The second column        x[1] is 'completion_timestamp'
   hour_offset0 = temporal_offset()
   hour_offset1 = temporal_offset()

   return x[0] + timedelta(hours=hour_offset0), x[1] + timedelta(hours=hour_offset1)
   

def spatial_offset(N, range_m=500):
# create a random numnber between -Hours and +Hours
    range_random = np.random.rand(N)*range_m*2 - range_m
    azimuth_random = np.random.rand(N)*180*2 - 180
    return range_random, azimuth_random

 
def main():
    # Step 1.  Compute the centroid for belville south 
    centroid = compute_belville_south_centroid()

    # Step 2.  Load CHALLENGE_2_OUTPUT: sr_hex_joined with the H3 Level 8 indice    
    is_service_data_downloaded = False
    try:
        process_start_time = timeit.default_timer()
        with open(CHALLENGE_2_OUTPUT) as f_:
            sr_hex_joined = pd.read_csv(f_)

        time_elapsed = timeit.default_timer() - process_start_time
        logger.info(f"'{CHALLENGE_2_OUTPUT}' loaded. Time Taken: {time_elapsed}s")
        is_service_data_downloaded = True
   
    except FileNotFoundError:
        logger.exception(f"Cannot read/write: '{CHALLENGE_2_OUTPUT}'")
    except PermissionError:
        logger.exception(f"Permission error: '{CHALLENGE_2_OUTPUT}'") 

    # Step 3.  Create subsample of the CHALLENGE_2_OUTPUT
    is_merged = False
    if is_service_data_downloaded:
       process_start_time = timeit.default_timer()
       is_within_1_minute_of_centroid = sr_hex_joined.apply(
         check_if_within_1_minute_of_centroid, 
         axis=1, 
         args=(centroid[0], centroid[1])
         )
       sr_hex_joined = sr_hex_joined[is_within_1_minute_of_centroid]
       time_elapsed = timeit.default_timer() - process_start_time
       logger.info(f"Subsample created. Time Taken: {time_elapsed}s")
   
    
       # Step 4.  Download and prepare wind data from WIND_DATA_SOURCE
       wind_speed_df = extract_belville_wind_data()
       wind_speed_df["date_and_time"] = pd.to_datetime(wind_speed_df["date_and_time"])
       wind_speed_df_sorted = wind_speed_df.sort_values(by='date_and_time')
  
       sr_hex_joined["creation_timestamp"] = pd.to_datetime(sr_hex_joined["creation_timestamp"])
       sr_hex_sorted = sr_hex_joined.sort_values(by='creation_timestamp')

       # Step 5.  Join Wind Data from the Bellville South Air Quality Measurement site 
       process_start_time = timeit.default_timer()
       sr_hex_merged = pd.merge_asof(
             sr_hex_sorted,
             wind_speed_df_sorted,
             left_on="creation_timestamp",
             right_on="date_and_time",
         )
   
       time_elapsed = timeit.default_timer() - process_start_time
       logger.info(f"Subsample merged with wind data. Time Taken: {time_elapsed}s")
       is_merged = True  
       
       try:
           sr_hex_merged.to_csv(CHALLENGE_5_TMP_OUTPUT, index=False)
           
       except FileNotFoundError:
           logger.exception(f"Cannot read/write: '{CHALLENGE_5_TMP_OUTPUT}'")
       except PermissionError:
           logger.exception(f"Permission error: '{CHALLENGE_5_TMP_OUTPUT}'") 
   
    # Step 6.  Anonymise dataframe and save dataframe to disk
    if is_merged:
       
      process_start_time = timeit.default_timer()
      # 'reference_number' and 'notification_number' are removed as these may be
      # be used to trace back to the customer
      sr_hex_merged.pop('notification_number') 
      sr_hex_merged.pop('reference_number') 
      
      # 'date_and_time' is removed - used to debug/test the merge of data frames
      sr_hex_merged.pop('date_and_time') 

      sr_hex_merged["creation_timestamp"] = pd.to_datetime(sr_hex_merged["creation_timestamp"])
      sr_hex_merged["completion_timestamp"] = pd.to_datetime(sr_hex_merged["completion_timestamp"])

      # temporal accuracy to within 6 hours
      temporal_data = sr_hex_merged.apply(anonomise_timestamp, axis=1)
      data_creation_timestamp = list()
      data_completion_timestamp = list()
      for t in temporal_data:
          data_creation_timestamp.append(t[0])
          data_completion_timestamp.append(t[1])

      # location accuracy to within approximately 500m
      range_random, azimuth_random = spatial_offset(len(sr_hex_merged))

      input_lat  = sr_hex_merged["latitude"]
      input_lon  = sr_hex_merged["longitude"]
      geod = pyproj.Geod(ellps='WGS84')

      lon_random, lat_random, reverse_az_random = geod.fwd(
        input_lon, 
        input_lat, 
        azimuth_random, 
        range_random,
        )
        
      sr_hex_merged["latitude"] = input_lat
      sr_hex_merged["longitude"] = input_lon

      try:
          sr_hex_merged.to_csv(CHALLENGE_5_OUTPUT, index=False)
          time_elapsed = timeit.default_timer() - process_start_time
          logger.info(f"Data aonymised and saved: '{CHALLENGE_5_OUTPUT}'. Time Taken: {time_elapsed}s")
      
      except FileNotFoundError:
          logger.exception(f"Cannot read/write: '{CHALLENGE_5_OUTPUT}'")
      except PermissionError:
          logger.exception(f"Permission error: '{CHALLENGE_5_OUTPUT}'") 
          
if __name__ == "__main__":
    # This will delete all cached files and force all downloads
    delete_cached_files = True    # set to False if cached downloads are used.  
    is_success = True
    if delete_cached_files:
        is_success = delete_file(WIND_DATA_SOURCE)
        is_success = is_success and delete_file(WIND_DATA_OUTPUT)
        is_success = is_success and delete_file(CHALLENGE_5_TMP_WIND_DATA)
        is_success = is_success and delete_file(CHALLENGE_5_TMP_OUTPUT)
        is_success = is_success and delete_file(CHALLENGE_5_OUTPUT)
        logger.stop()
        is_success = is_success and delete_file(CHALLENGE_5_LOG)
      
    if is_success:  
        # Start timer
        start_time = timeit.default_timer()
    
        # Set logger
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        sys.tracebacklimit = 0
        logger.add(CHALLENGE_5_LOG, level="DEBUG", rotation="12:00")
    
        logger.info("Starting Challenge #5")
        main()
        time_elapsed = timeit.default_timer() - start_time
        logger.info(f"Challenge #5 Completed. Total Time Taken: {time_elapsed}s")     
        logger.stop()
    else:
        logger.error("Please close cached files and rerun.")