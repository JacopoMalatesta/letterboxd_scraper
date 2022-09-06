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

    def get_boto_session(self) -> boto3.session.Session:
        return boto3.Session(aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_access_key)

    @staticmethod
    def get_s3_client(session: boto3.session.Session) -> boto3.session.Session.client:
        return session.client('s3')

    @staticmethod
    def get_s3_resource(session: boto3.session.Session) -> boto3.session.Session.resource:
        return session.resource('s3')

    @time_it
    def read_csv_from_s3(self,
                         s3_client,
                         key: str) -> pd.DataFrame or None:
        try:
            obj = s3_client.get_object(Bucket=self.bucket, Key=key)
            buffer = io.BytesIO(obj['Body'].read())
            return pd.read_csv(buffer)
        except ClientError:
            info_log.info("Scraping this playlist for the first time.")
            return None

    @time_it
    def write_csv_to_s3(self,
                        s3_resource,
                        filename: str,
                        df: pd.DataFrame):
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource.Object(self.bucket, f'{filename}').put(Body=csv_buffer.getvalue())


def get_s3_key(metadata: dict) -> str:
    """Returns the S3 key to the dataframe"""
    return metadata["user"] + "/" + metadata["title"] + "/" + metadata["title"] + ".csv"
