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

    @time_it
    def read_csv_from_s3(self,
                         key: str) -> pd.DataFrame or None:

        try:
            session = boto3.Session(aws_access_key_id=self.access_key,
                                    aws_secret_access_key=self.secret_access_key)
            s3 = session.client('s3')
            obj = s3.get_object(Bucket=self.bucket, Key=key)
            buffer = io.BytesIO(obj['Body'].read())
            return pd.read_csv(buffer)
        except ClientError:
            return None

    @time_it
    def write_csv_to_s3(self,
                        filename: str,
                        df: pd.DataFrame):

        session = boto3.Session(aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_access_key)

        s3_resource = session.resource('s3')
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource.Object(self.bucket, f'{filename}').put(Body=csv_buffer.getvalue())
        info_log.info(f"Dataframe in S3 bucket will have {len(df)} records")


def get_s3_key(metadata: dict) -> str:
    """Returns the S3 key to the dataframe"""
    return metadata["user"] + "/" + metadata["title"] + ".csv"
