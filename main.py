# IMPROVEMENTS/NICE TO HAVE
# 2. Solve encoding issue
# 3. Amazon Athena and connect to Tableau
# 4. AWS Lambda
# 7. Add docstrings + type declarations
# 8. Unit testing
# 9. Push to git

import sys
import logging
import argparse
from utils.config import config_dict
from utils.playlist_metadata import Playlist
from utils.letterboxd_scraping_functions import get_url_for_each_page, parse_playlist_pages_as_html, \
    get_soup_objects_from_playlist_pages, scrape_ids_ratings_and_urls, parse_film_urls_as_html, \
    get_soup_objects_from_film_pages, scrape_remaining_film_data
from utils.s3_utils import get_s3_key, read_current_df_from_s3, write_final_df_to_s3
from utils.data_wrangling_utils import get_new_records, create_final_dataframe
from utils.logging_utils import Logger
from utils.time_utils import time_it
from utils.generic_scraping_functions import ParallelTechnique
from utils.argparse_utils import format_how_argument

sys.setrecursionlimit(100000)

info_log = Logger(name=__name__, level=logging.INFO).return_logger()


@time_it
def main(url: str,
         asynchronous: str,
         parallel_technique: ParallelTechnique,
         over_write: str = "false"):

    if __name__ == '__main__':

        playlist = Playlist(url=url)
        metadata = playlist.get_playlist_metadata()

        info_log.info(f"Started scraping playlist {metadata['title'].upper()} from user {metadata['user'].upper()}")

        pages_urls = get_url_for_each_page(playlist_url=metadata["url"], number_of_pages=metadata["number_of_pages"])

        html_pages, runtime = parse_playlist_pages_as_html(pages_urls=pages_urls, asynchronous=asynchronous)
        info_log.info(f"Finished asynchronously parsing playlist pages as HTML code in {runtime}")

        pages_soups, runtime = get_soup_objects_from_playlist_pages(html_pages=html_pages,
                                                                    technique=parallel_technique)
        info_log.info(f"Finished getting BeautifulSoup objects out of playlist pages with in {runtime}")

        ids_ratings_urls, runtime = scrape_ids_ratings_and_urls(pages_soups=pages_soups, playlist_type=metadata["type"])
        info_log.info(f"Finished scraping film IDs, ratings and URLs from playlist pages in {runtime}")

        df_key = get_s3_key(username=metadata["user"], playlist_title=metadata["title"])

        current_df, runtime = read_current_df_from_s3(configs=config_dict, key=df_key)
        info_log.info(f"Finished reading the current dataframe from S3 (if any) in {runtime}")

        current_df = None if over_write == "true" else current_df

        new_records = get_new_records(current_df=current_df,
                                      film_ids=ids_ratings_urls["ids"],
                                      film_urls=ids_ratings_urls["urls"])

        html_pages, runtime = parse_film_urls_as_html(current_df=current_df,
                                                      new_records=new_records,
                                                      film_urls=ids_ratings_urls["urls"],
                                                      asynchronous=asynchronous)
        info_log.info(f"Finished asynchronously parsing film pages as HTML code in {runtime}")

        film_soups, runtime = get_soup_objects_from_film_pages(current_df=current_df,
                                                               new_records=new_records,
                                                               html_pages=html_pages,
                                                               technique=parallel_technique)
        info_log.info(f"Finished getting BeautifulSoup objects from HTML pages with in {runtime}")

        more_film_data, runtime = scrape_remaining_film_data(current_df=current_df,
                                                             new_records=new_records,
                                                             film_soups=film_soups)
        info_log.info(f"Finished scraping all other film data in {runtime}")

        final_df = create_final_dataframe(current_df=current_df,
                                          new_records=new_records,
                                          ids_ratings_urls=ids_ratings_urls,
                                          additional_film_data=more_film_data)

        _none, runtime = write_final_df_to_s3(configs=config_dict, df=final_df, key=df_key)
        info_log.info(f"Finished writing final dataframe in S3 bucket in {runtime}")


parser = argparse.ArgumentParser()

parser.add_argument("--url",
                    help="Playlist URL to scrape",
                    type=str)
parser.add_argument("--asynchronous",
                    help="If true the URLs are parsed asynchronously",
                    type=str,
                    default="true")
parser.add_argument("--how",
                    help="Allows user to choose between multithreading and multiprocessing",
                    type=str,
                    default="multiprocessing")
parser.add_argument("--overwrite",
                    help="If True it scrapes the entire playlist and overwrites the existing csv file in S3",
                    type=str,
                    default="false")

args = parser.parse_args()

playlist_url = args.url

asynchronous = args.asynchronous

how = format_how_argument(how=args.how)

overwrite = args.overwrite


if __name__ == "__main__":
    test, total_execution_time = main(url=playlist_url,
                                      asynchronous=asynchronous,
                                      parallel_technique=how,
                                      over_write=overwrite)
    info_log.info(f"Overall runtime {how.value.upper()}: {total_execution_time}")
