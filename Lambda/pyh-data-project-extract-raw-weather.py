import os
import time
from datetime import datetime
import pandas as pd
import requests
import boto3
import io

CONFIG = {
    "api": {
        "key": "KEY",
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

            if len(results) < limit:
                break

            offset += limit
            time.sleep(CONFIG['api']['request_delay'])

        except requests.exceptions.RequestException:
            break

    return all_results


def save_to_s3(df, filename):
    if df.empty:
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
    except Exception:
        pass


def main():
    try:
        datatypes = fetch_api_data("datatypes")
        datatypes_df = pd.DataFrame(datatypes)
        filtered_df = datatypes_df[datatypes_df['id'].isin(CONFIG['data']['datatype_ids'])].copy()
        save_to_s3(filtered_df, "datatype.csv")

        datasets = fetch_api_data("datasets")
        save_to_s3(pd.DataFrame(datasets), "dataset.csv")

        stations = fetch_api_data("stations", {"locationid": CONFIG['data']['location_id']})
        stations_df = pd.DataFrame(stations)
        save_to_s3(stations_df, "station.csv")

        station_ids = stations_df["id"].tolist() if not stations_df.empty else []
        if not station_ids:
            return

        all_weather_data = []

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

                    year_data = fetch_api_data("data", params)
                    all_weather_data.extend(year_data)

        if all_weather_data:
            weather_df = pd.DataFrame(all_weather_data)
            weather_df["date"] = pd.to_datetime(weather_df["date"])
            weather_df.sort_values("date", inplace=True)
            save_to_s3(weather_df, "weather.csv")

    except Exception:
        pass


def lambda_handler(event, context):
    main()
    return
