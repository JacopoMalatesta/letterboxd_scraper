import io
import logging
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from utils.time_utils import time_it
from utils.logging_utils import Logger


info_log = Logger(name=__name__, level=logging.INFO).return_logger()


def get_boto_session(config: dict) -> boto3.session.Session:
    """Returns a Boto3 session"""
    return boto3.Session(aws_access_key_id=config["aws_access_key"],
                         aws_secret_access_key=config["aws_secret_access_key"])


def get_s3_client(session: boto3.session.Session) -> boto3.session.Session.client:
    """Returns an S3 client"""
    return session.client('s3')


def get_s3_resource(session: boto3.session.Session) -> boto3.session.Session.resource:
    """Returns an s3 resource"""
    return session.resource('s3')


def get_s3_key(metadata: dict) -> str:
    """Returns the S3 key to the dataframe"""
    return metadata["user"] + "/" + metadata["title"] + "/" + metadata["title"] + ".csv"


@time_it
def read_csv_from_s3(bucket: str,
                     s3_client: boto3.session.Session.client,
                     key: str) -> pd.DataFrame or None:
    """Reads the .csv file containing the playlist from S3"""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(obj['Body'].read())
        return pd.read_csv(buffer)
    except ClientError:
        info_log.info("Scraping this playlist for the first time.")
        return None


@time_it
def write_csv_to_s3(bucket: str,
                    s3_resource: boto3.session.Session.resource,
                    filename: str,
                    df: pd.DataFrame):
    """Writes a Pandas dataframe to S3 as a .csv file"""
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket, f'{filename}').put(Body=csv_buffer.getvalue())
