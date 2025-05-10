import os
import logging

import pandas as pd
from clean import clean_parquet_path

from sqlalchemy import create_engine


def save_data_to_db(**context):
    database_url = context["var"]["value"].get("DATABASE_URL")
    df = pd.read_parquet(clean_parquet_path)
    engine = create_engine(database_url)
    df.to_sql('bike_rides', con=engine, if_exists='append', index=False)
