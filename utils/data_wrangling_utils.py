import pandas as pd
import logging
from utils.logging_utils import Logger

info_log = Logger(name=__name__, level=logging.INFO).return_logger()


def cast_id_and_year_as_numeric(current_df: pd.DataFrame) -> pd.DataFrame:
    """Typecasts the ID and year series to numeric"""
    current_df[["id", "year"]] = current_df[["id", "year"]].apply(lambda col: pd.to_numeric(col))
    return current_df


def get_film_ids_in_current_df(current_df: pd.DataFrame) -> pd.Series:
    """Extracts the IDs of the films currently present in the S3 bucket"""
    return current_df["id"].unique()


def get_new_records(current_film_ids: pd.Series,
                    ids_ratings_urls_dict: dict) -> list[str]:
    """Creates a list with all IDs of films that have been added to the playlist after the latest upload"""
    new_entries = []
    for film_id, url in zip(ids_ratings_urls_dict["ids"], ids_ratings_urls_dict["urls"]):
        if film_id not in current_film_ids:
            new_entries.append(url)
    info_log.info(f"N. of new records: {len(new_entries)}")
    return new_entries


def get_ratings_dataframe(ids_ratings_urls_dict: dict) -> pd.DataFrame:
    """Creates a dataframe out of the dictionary storing the IDs, rating and urls, renames two columns and drops the
    url key/column"""
    return pd.DataFrame(ids_ratings_urls_dict).rename(columns={'ids': 'id', 'ratings': 'rating'}).drop(columns='urls')


def get_film_data_dataframe(additional_film_data: dict) -> pd.DataFrame:
    """Stores the additional film data dictionary in a dataframe and renames some of its keys/columns"""
    df = pd.DataFrame(additional_film_data)
    return df.rename(columns={'film_ids': 'id', 'titles': 'title', 'years': 'year', 'directors': 'director'})


def inner_join_two_dataframes_on_film_id(left_dataframe: pd.DataFrame,
                                         right_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Merges the ratings dataframe to the film data dataframe with an inner join on film ID"""
    return pd.merge(left=left_dataframe, right=right_dataframe, how="inner", on="id")


def sort_dataframe_by_year_and_title(final_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Sorts the supplied dataframe first by year and then by title, both in ascending order"""
    return final_dataframe.sort_values(["year", "title"])


def get_all_columns_except_ratings_from_current_dataframe(current_df: pd.DataFrame) -> pd.DataFrame:
    """Selects all columns from the current dataframe except ratings"""
    return current_df[["id", "title", "year", "director", "actors", "countries"]]


def append_new_records_to_current_dataframe(current_df_no_ratings: pd.DataFrame,
                                            new_records_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Concatenates the new records dataframe to the current dataframe"""
    return pd.concat([current_df_no_ratings, new_records_dataframe], ignore_index=True)
