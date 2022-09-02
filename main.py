# IMPROVEMENTS/NICE TO HAVE
# 1. Solve encoding issue
# 2. Amazon Athena and connect to Tableau
# 3. AWS Lambda
# 4. Add docstrings + type declarations
# 5. Unit testing

import sys
import logging
import argparse
from distutils.util import strtobool
from utils.config import config_dict
from utils.playlist_metadata import Playlist
from utils.generic_scraping_functions import parse_all_urls_asynchronously, get_soup_objects_multiprocessing,\
    get_soup_objects_multithreading, get_soup_objects_synchronously
from utils.letterboxd_scraping_functions import get_url_for_each_page, scrape_ids_ratings_and_urls,\
    scrape_remaining_film_data
from utils.s3_utils import get_s3_key, read_current_df_from_s3, write_final_df_to_s3
from utils.data_wrangling_utils import get_film_ids_in_current_df, get_new_records, get_ratings_dataframe,\
    get_film_data_dataframe, inner_join_two_dataframes_on_film_id, sort_dataframe_by_year_and_title,\
    get_all_columns_except_ratings_from_current_dataframe, append_new_records_to_current_dataframe
from utils.logging_utils import Logger
from utils.time_utils import time_it
from utils.enums_classes import ParallelTechnique
from utils.argparse_utils import format_soupification_argument

sys.setrecursionlimit(100000)

info_log = Logger(name=__name__, level=logging.INFO).return_logger()


@time_it
def main(url: str,
         parallel_technique: ParallelTechnique,
         over_write: bool):

    if __name__ == '__main__':

        playlist = Playlist(url=url)
        metadata = playlist.get_playlist_metadata()

        info_log.info(f"Started scraping playlist {metadata['title'].upper()} from user {metadata['user'].upper()}")

        playlist_pages_urls = get_url_for_each_page(metadata=metadata)

        playlist_pages_html, runtime = parse_all_urls_asynchronously(urls=playlist_pages_urls)

        info_log.info(f"Asynchronously parsed playlist pages as HTML code in {runtime}")

        if parallel_technique == ParallelTechnique.MULTIPROCESSING:
            playlist_pages_soups, runtime = get_soup_objects_multiprocessing(html_pages=playlist_pages_html)
        elif parallel_technique == ParallelTechnique.MULTITHREADING:
            playlist_pages_soups, runtime = get_soup_objects_multithreading(html_pages=playlist_pages_html)
        else:
            playlist_pages_soups, runtime = get_soup_objects_synchronously(html_pages=playlist_pages_html)

        info_log.info(f"Got BeautifulSoup objects out of playlist pages ({parallel_technique.value}) in {runtime}")

        ids_ratings_urls_dict, runtime = scrape_ids_ratings_and_urls(pages_soups=playlist_pages_soups)

        info_log.info(f"Scraped film IDs, ratings and URLs from playlist pages in {runtime}")

        df_key = get_s3_key(metadata=metadata)

        current_df, runtime = read_current_df_from_s3(configs=config_dict, key=df_key)
        info_log.info(f"Read the current dataframe from S3 (if any) in {runtime}")

        current_df = None if over_write else current_df

        if current_df is not None:
            current_film_ids = get_film_ids_in_current_df(current_df=current_df)
            new_records = get_new_records(current_film_ids=current_film_ids,
                                          ids_ratings_urls_dict=ids_ratings_urls_dict)
        else:
            new_records = None

        if new_records:
            film_pages_html, runtime = parse_all_urls_asynchronously(new_records)
            info_log.info(f"Asynchronously parsed newly added films as HTML code in {runtime}")
        elif current_df is None:
            film_pages_html, runtime = parse_all_urls_asynchronously(ids_ratings_urls_dict["urls"])
            info_log.info(f"Asynchronously parsed all film pages as HTML code in {runtime}")
        else:
            film_pages_html = None

        if current_df is None or new_records:
            if parallel_technique == ParallelTechnique.MULTIPROCESSING:
                film_pages_soups, runtime = get_soup_objects_multiprocessing(html_pages=film_pages_html)
            elif parallel_technique == ParallelTechnique.MULTITHREADING:
                film_pages_soups, runtime = get_soup_objects_multithreading(html_pages=film_pages_html)
            else:
                film_pages_soups, runtime = get_soup_objects_synchronously(html_pages=film_pages_html)

            info_log.info(f"Got BeautifulSoup objects out of HTML film pages ({parallel_technique.value}) "
                          f"in {runtime}")
        else:
            film_pages_soups = None

        if current_df is None or new_records:
            more_film_data, runtime = scrape_remaining_film_data(film_soups=film_pages_soups)
            info_log.info(f"Scraped all other film data in {runtime}")
        else:
            more_film_data = None

        ratings_df = get_ratings_dataframe(ids_ratings_urls_dict=ids_ratings_urls_dict)

        if current_df is None:
            film_data_df = get_film_data_dataframe(additional_film_data=more_film_data)
            joined_df = inner_join_two_dataframes_on_film_id(left_dataframe=ratings_df,
                                                             right_dataframe=film_data_df)

        else:
            if new_records:
                new_records_dataframe = get_film_data_dataframe(additional_film_data=more_film_data)
                current_df_no_ratings = get_all_columns_except_ratings_from_current_dataframe(current_df=current_df)
                appended_df = append_new_records_to_current_dataframe(current_df_no_ratings=current_df_no_ratings,
                                                                      new_records_dataframe=new_records_dataframe)
                joined_df = inner_join_two_dataframes_on_film_id(left_dataframe=ratings_df,
                                                                 right_dataframe=appended_df)
            else:
                current_df_no_ratings = get_all_columns_except_ratings_from_current_dataframe(current_df=current_df)
                joined_df = inner_join_two_dataframes_on_film_id(left_dataframe=ratings_df,
                                                                 right_dataframe=current_df_no_ratings)

        final_df = sort_dataframe_by_year_and_title(final_dataframe=joined_df)
        info_log.info(f"Created the final dataframe")

        _none, runtime = write_final_df_to_s3(configs=config_dict, df=final_df, key=df_key)
        info_log.info(f"Wrote final dataframe to S3 bucket in {runtime}")


parser = argparse.ArgumentParser()

parser.add_argument("url",
                    help="URL of the playlist to scrape",
                    type=str)
parser.add_argument("-s",
                    "--soupification",
                    help="Allows user to decide how to turn HTML pages into BeautifulSoup objects",
                    type=str,
                    default="synchronous",
                    choices=("multiprocessing", "multithreading", "synchronous"))
parser.add_argument("-o",
                    "--overwrite",
                    help="If True it scrapes the entire playlist and overwrites the existing csv file in S3",
                    type=lambda x: bool(strtobool(x)),
                    default=False)

args = parser.parse_args()
playlist_url = args.url
soupification = format_soupification_argument(args.soupification)
overwrite = args.overwrite


if __name__ == "__main__":
    test, total_execution_time = main(url=playlist_url,
                                      parallel_technique=soupification,
                                      over_write=overwrite)
    info_log.info(f"Total runtime: {total_execution_time}")
