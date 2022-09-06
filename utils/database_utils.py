import sqlalchemy
import pandas as pd
from utils.time_utils import time_it


def get_engine(config: dict) -> sqlalchemy.engine:
    string = f"postgresql://{config['local_db_username']}:{config['local_db_password']}@{config['local_db_host']}:" \
             f"{config['local_db_port']}/{config['local_db_name']}"
    return sqlalchemy.create_engine(string)


def get_table_name(playlist_metadata: dict) -> str:
    return playlist_metadata["user"] + "_" + playlist_metadata["title"]


@time_it
def write_to_database(engine: sqlalchemy.engine, df: pd.DataFrame, table_name: str):
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
