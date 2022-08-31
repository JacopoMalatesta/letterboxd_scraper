import sqlalchemy


def get_engine(username: str, password: str, host: str, port: int, database: str) -> sqlalchemy.engine.base.Engine:
    engine = sqlalchemy.create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
    return engine



