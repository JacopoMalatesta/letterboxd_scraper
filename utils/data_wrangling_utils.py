import pandas as pd
import logging
from utils.logging_utils import Logger

info_log = Logger(name=__name__, level=logging.INFO).return_logger()


def get_new_records(current_df: pd.DataFrame,
                    film_ids: list[int],
                    film_urls: list[str]) -> list[str] or None:
    """Creates a list with all IDs of films that have been added to the playlist after the latest upload"""
    if current_df is not None:
        current_film_ids = current_df["id"].unique()
        new_entries = []
        for film_id, url in zip(film_ids, film_urls):
            if film_id not in current_film_ids:
                new_entries.append(url)
        info_log.info(f"N. of new records: {len(new_entries)}")
        return new_entries
    else:
        info_log.info("Downloading all records from playlists")
        return None


def create_final_dataframe(current_df: pd.DataFrame,
                           new_records: list[str],
                           ids_ratings_urls: dict,
                           additional_film_data: dict) -> pd.DataFrame:

    ratings_dict = {"id": ids_ratings_urls["ids"],
                    "rating": ids_ratings_urls["ratings"]}

    ratings_df = pd.DataFrame(ratings_dict)

    if current_df is None:
        no_ratings_dict = {"id": additional_film_data["film_ids"],
                           "title": additional_film_data["titles"],
                           "year": additional_film_data["years"],
                           "director": additional_film_data["directors"],
                           "actors": additional_film_data["actors"],
                           "countries": additional_film_data["countries"]}

        no_ratings_df = pd.DataFrame(no_ratings_dict)

        return pd.merge(left=ratings_df, right=no_ratings_df, how="inner", on="id").sort_values(["year", "title"])

    else:
        if new_records:

            new_entries_dict = {"id": additional_film_data["film_ids"],
                                "title": additional_film_data["titles"],
                                "year": additional_film_data["years"],
                                "director": additional_film_data["directors"],
                                "actors": additional_film_data["actors"],
                                "countries": additional_film_data["countries"]}

            df_new_entries = pd.DataFrame(new_entries_dict)

            current_df_no_ratings = current_df[["id", "title", "year", "director", "actors", "countries"]]

            df_two = pd.concat([current_df_no_ratings, df_new_entries], ignore_index=True)

            return pd.merge(ratings_df, df_two, "inner", "id").sort_values(["year", "title"])

        else:
            current_df_no_ratings = current_df[["id", "title", "year", "director", "actors", "countries"]]
            return pd.merge(ratings_df, current_df_no_ratings, "inner", "id").sort_values(["year", "title"])