import os


config_dict = {"local_db_username": os.environ["postgres_username"],
               "local_db_password": os.environ["postgres_psw"],
               "local_db_host": os.environ["postgres_host"],
               "local_db_port": os.environ["postgres_port"],
               "aws_access_key": os.environ["aws_access_key"],
               "aws_secret_access_key": os.environ["aws_secret_access_key"],
               "s3_bucket": os.environ["aws_s3_bucket"]}



