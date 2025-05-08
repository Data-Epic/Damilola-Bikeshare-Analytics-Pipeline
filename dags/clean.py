import os
import logging
import pandas as pd
import plotly.express as px

clean_parquet_path = '/opt/airflow/dags/data/bikeshare_cleaned.parquet'


def clean_data():
    try:
        df = pd.read_csv('/opt/airflow/dags/data/202212-capitalbikeshare-tripdata.csv')
        df['started_at'] = pd.to_datetime(df['started_at'])
        df['ended_at'] = pd.to_datetime(df['ended_at'])
        df['trip_duration_minutes'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60

        df['day_of_week'] = df['started_at'].dt.day_name()
        df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])
        missing_stations = df['start_station_name'].isna() | df['end_station_name'].isna()

        df['has_missing_station'] = missing_stations

        invalid_duration = df['trip_duration_minutes'] <= 0
        df = df[~invalid_duration]

        invalid_times = df['ended_at'] < df['started_at']
        df = df[~invalid_times]

        df['year'] = df['started_at'].dt.year
        df['week'] = df['started_at'].dt.isocalendar().week
        df['week'] = df['week'].astype(int)
        df.to_parquet(clean_parquet_path, index=False)
        print("cleaning done")

    except Exception as e:
        raise


def partition_by_parquet():
    df = pd.read_parquet(clean_parquet_path)
    for _, row in df.iterrows():
        member_type = row['member_casual']
        week = row['week']
        path = f'/opt/airflow/dags/data/output/member_type={member_type}/week={week}/'
        os.makedirs(path, exist_ok=True)
        file_path = os.path.join(path, "bikeshare_cleaned.parquet")
        pd.DataFrame([row]).to_parquet(file_path, index=False)


def generate_heatmap():
    df = pd.read_parquet(clean_parquet_path)
    df = df.dropna(subset=["start_lat", "start_lng"])

    fig = px.density_map(
        df,
        lat="start_lat",
        lon="start_lng",
        radius=10,
        center=dict(lat=df["start_lat"].mean(), lon=df["start_lng"].mean()),
        zoom=11,
        map_style="carto-positron",  # No Mapbox token needed
        title="Bikeshare Start Location Heatmap"
    )

    output_dir = "/opt/airflow/dags/heatmap/"
    os.makedirs(output_dir, exist_ok=True)
    fig.write_html(os.path.join(output_dir, "heatmap.html"))


if __name__ == "__main__":
    # clean_data()
    generate_heatmap()
