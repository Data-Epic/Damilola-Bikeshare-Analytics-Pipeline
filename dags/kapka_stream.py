import pandas as pd
import time
import logging
from clean import clean_parquet_path

def stream_data_and_log_alert():
    logger = logging.getLogger(__name__)
    df = pd.read_parquet(clean_parquet_path)

    def trip_stream(df_):
        for _, row in df_.iterrows():
            yield row
            time.sleep(0.5)

    for trip in trip_stream(df):
        duration_minutes = trip['trip_duration_minutes']
        start_hour = trip['started_at'].hour

        if duration_minutes > 45:
            logger.warning(f"⚠️ ALERT: Long ride detected! Duration: {duration_minutes:.2f} minutes")
        if trip['member_casual'] == 'casual' and start_hour == 0:
            logger.warning("⚠️ ALERT: Casual rider started a trip at midnight!")
