import io
import logging
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from dataclasses import dataclass
from utils.time_utils import time_it
from utils.logging_utils import Logger


info_log = Logger(name=__name__, level=logging.INFO).return_logger()


@dataclass
class Bucket:
    bucket: str
    access_key: str
    secret_access_key: str

    def read_csv_from_s3(self,
                         key: str) -> pd.DataFrame:

        session = boto3.Session(aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_access_key)

        s3 = session.client('s3')
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        buffer = io.BytesIO(obj['Body'].read())
        return pd.read_csv(buffer)

    def write_csv_to_s3(self,
                        filename: str,
                        df: pd.DataFrame) -> None:

        session = boto3.Session(aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_access_key)

        s3_resource = session.resource('s3')
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource.Object(self.bucket, f'{filename}').put(Body=csv_buffer.getvalue())


def get_s3_key(username: str, playlist_title: str) -> str:
    """Returns the S3 key to the dataframe"""
    return username + "/" + playlist_title + ".csv"


@time_it
def read_current_df_from_s3(configs: dict, key: str) -> pd.DataFrame or None:
    """If present, it reads the playlist currently stored in S3. If not it returns None"""

    s3_bucket = Bucket(bucket=configs["s3_bucket"],
                       access_key=configs["aws_access_key"],
                       secret_access_key=configs["aws_secret_access_key"])

    try:
        current_df = s3_bucket.read_csv_from_s3(key=key)
    except ClientError:
        current_df = None
    else:
        cols = ["id", "year"]
        current_df[cols] = current_df[cols].apply(lambda col: pd.to_numeric(col))
    return current_df


@time_it
def write_final_df_to_s3(configs: dict, df: pd.DataFrame, key: str):
    """Writes the final dataframe to S3"""

    s3_bucket = Bucket(bucket=configs["s3_bucket"],
                       access_key=configs["aws_access_key"],
                       secret_access_key=configs["aws_secret_access_key"])

    s3_bucket.write_csv_to_s3(filename=key,
                              df=df)
    info_log.info(f"Dataframe in S3 bucket will have {len(df)} records")
