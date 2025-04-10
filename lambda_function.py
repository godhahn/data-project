import os
import time
import logging
from datetime import datetime
import pandas as pd
import requests
import boto3
import io

CONFIG = {
    "api": {
        "key": "",
        "base_url": "https://www.ncei.noaa.gov/cdo-web/api/v2/",
        "request_delay": 0.5,
    },
    "data": {
        "s3_bucket": "pyh-data-project-bucket",
        "s3_prefix": "raw_weather/",
        "location_id": "CITY:SN000001",
        "datatype_ids": ["TAVG", "TMAX", "TMIN", "HPCP", "PRCP", "AWND", "ALL"],
        "dataset_id": "GHCND",
        "start_date": datetime(2020, 1, 1).date(),
        "end_date": datetime.now().date(),
    }
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

s3_client = boto3.client('s3')


def fetch_api_data(endpoint, params=None):
    all_results = []
    limit, offset = 1000, 1
    
    while True:
        request_params = {'limit': limit, 'offset': offset}
        if params:
            request_params.update(params)
            
        try:
            response = requests.get(
                CONFIG['api']['base_url'] + endpoint,
                headers={"token": CONFIG['api']['key']},
                params=request_params,
                timeout=30
            )
            response.raise_for_status()
            
            results = response.json().get('results', [])
            if not results:
                break
                
            all_results.extend(results)
            logging.info(f"Fetched {len(results)} records from {endpoint}, total: {len(all_results)}")
            
            if len(results) < limit:
                break
                
            offset += limit
            time.sleep(CONFIG['api']['request_delay'])
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {endpoint}: {str(e)}")
            break
            
    return all_results


def save_to_s3(df, filename):
    if df.empty:
        logging.info(f"Dataframe is empty, not saving {filename}")
        return
        
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3_key = f"{CONFIG['data']['s3_prefix']}{filename}"
        
        s3_client.put_object(
            Bucket=CONFIG['data']['s3_bucket'],
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        logging.info(f"Saved {len(df)} records to s3://{CONFIG['data']['s3_bucket']}/{s3_key}")
    except Exception as e:
        logging.error(f"Error saving to S3: {str(e)}")


def main():
    try:
        logging.info(f"Starting NOAA data fetch from {CONFIG['data']['start_date']} to {CONFIG['data']['end_date']}")
        
        # Fetch and save datatypes
        logging.info("Fetching datatypes...")
        datatypes = fetch_api_data("datatypes")
        datatypes_df = pd.DataFrame(datatypes)
        filtered_df = datatypes_df[datatypes_df['id'].isin(CONFIG['data']['datatype_ids'])].copy()
        save_to_s3(filtered_df, "datatype.csv")
        
        # Fetch and save datasets
        logging.info("Fetching datasets...")
        datasets = fetch_api_data("datasets")
        save_to_s3(pd.DataFrame(datasets), "dataset.csv")
        
        # Fetch and save stations
        logging.info(f"Fetching stations for {CONFIG['data']['location_id']}...")
        stations = fetch_api_data("stations", {"locationid": CONFIG['data']['location_id']})
        stations_df = pd.DataFrame(stations)
        save_to_s3(stations_df, "station.csv")
        
        station_ids = stations_df["id"].tolist() if not stations_df.empty else []
        if not station_ids:
            logging.error("No stations found. Exiting.")
            return
            
        # Fetch weather data
        all_weather_data = []
        api_calls = 0
        
        for datatype_id in CONFIG['data']['datatype_ids']:
            for station_id in station_ids:
                for year in range(CONFIG['data']['start_date'].year, CONFIG['data']['end_date'].year + 1):

                    year_start = max(CONFIG['data']['start_date'], datetime(year, 1, 1).date())
                    year_end = min(CONFIG['data']['end_date'], datetime(year, 12, 31).date())
                    
                    if year_start > year_end:
                        continue
                    
                    params = {
                        "datasetid": CONFIG['data']['dataset_id'],
                        "datatypeid": datatype_id,
                        "stationid": station_id,
                        "startdate": year_start.strftime("%Y-%m-%d"),
                        "enddate": year_end.strftime("%Y-%m-%d"),
                        "units": "metric"
                    }
                    
                    logging.info(f"Fetching {datatype_id} for station {station_id} - {year}")
                    api_calls += 1
                    year_data = fetch_api_data("data", params)
                    all_weather_data.extend(year_data)
        
        logging.info(f"Made {api_calls} API calls to fetch weather data")
        
        if all_weather_data:
            weather_df = pd.DataFrame(all_weather_data)
            weather_df["date"] = pd.to_datetime(weather_df["date"])
            weather_df.sort_values("date", inplace=True)
            save_to_s3(weather_df, "weather.csv")
            logging.info(f"Weather data fetch completed successfully")
        else:
            logging.warning("No weather data retrieved")
            
    except Exception as e:
        logging.error(f"Process failed: {str(e)}")


def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': 'NOAA weather data fetched and saved to S3 successfully'
    }


if __name__ == "__main__":
    main()