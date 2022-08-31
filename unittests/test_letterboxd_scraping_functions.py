import unittest
from utils.playlist_metadata import Playlist
from utils.letterboxd_scraping_functions import *
from utils.generic_scraping_functions import ParallelTechnique
from utils.s3_utils import get_s3_key, read_current_df_from_s3
from utils.config import config_dict
from utils.data_wrangling_utils import get_new_records


class TestLetterboxdFunctions(unittest.TestCase):
    marino_blu_ray = Playlist(url="https://letterboxd.com/souchousama/list/my-blu-ray-collection/")
    marino_url = marino_blu_ray.url
    marino_n_pages = marino_blu_ray.get_number_of_pages()

    chiara_2021 = Playlist(url="https://letterboxd.com/sonochiara123/list/2021/by/added-earliest/")
    chiara_url = chiara_2021.url
    chiara_n_of_pages = chiara_2021.get_number_of_pages()

    marino_pages = get_url_for_each_page(playlist_url=marino_url,
                                         number_of_pages=marino_n_pages)

    chiara_pages = get_url_for_each_page(playlist_url=chiara_url,
                                         number_of_pages=chiara_n_of_pages)

    marino_htmls, runtime = parse_playlist_pages_as_html(pages_urls=marino_pages, asynchronous="true")

    marino_soups, runtime = get_soup_objects_from_playlist_pages(html_pages=marino_htmls,
                                                                 technique=ParallelTechnique.MULTITHREADING)

    ids_ratings_urls, runtime = scrape_ids_ratings_and_urls(pages_soups=marino_soups, playlist_type="list")

    ids, ratings, urls = ids_ratings_urls["ids"], ids_ratings_urls["ratings"], ids_ratings_urls["urls"]

    df_key = get_s3_key(username="souchousama", playlist_title="my_blu_ray_collection")

    current_df, runtime = read_current_df_from_s3(configs=config_dict, key=df_key)

    new_records = get_new_records(current_df=current_df,
                                  film_ids=ids_ratings_urls["ids"],
                                  film_urls=ids_ratings_urls["urls"])

    film_htmls_none, runtime = parse_film_urls_as_html(current_df=current_df,
                                                       new_records=None,
                                                       film_urls=urls,
                                                       asynchronous="true")
    film_htmls_full, runtime = parse_film_urls_as_html(current_df=None,
                                                       new_records=None,
                                                       film_urls=urls,
                                                       asynchronous="true")

    def test_get_url_for_each_page(self):
        self.assertEqual(len(TestLetterboxdFunctions.marino_pages), TestLetterboxdFunctions.marino_n_pages)
        self.assertEqual(len(TestLetterboxdFunctions.chiara_pages), TestLetterboxdFunctions.chiara_n_of_pages)

    def test_parse_playlist_pages_as_html(self):
        self.assertEqual(len(TestLetterboxdFunctions.marino_htmls), len(TestLetterboxdFunctions.marino_pages))
        self.assertIsInstance(TestLetterboxdFunctions.marino_htmls, list)
        self.assertIsInstance(TestLetterboxdFunctions.marino_htmls[0], str)

    def test_get_soup_objects_from_playlist_pages(self):
        self.assertEqual(len(TestLetterboxdFunctions.marino_soups), len(TestLetterboxdFunctions.marino_htmls))
        self.assertIsInstance(TestLetterboxdFunctions.marino_soups, list)
        self.assertIsInstance(TestLetterboxdFunctions.marino_soups[0], bs4.BeautifulSoup)

    def test_get_ids(self):
        self.assertIsInstance(TestLetterboxdFunctions.ids, list)
        self.assertIsInstance(TestLetterboxdFunctions.ids[0], int)
        self.assertEqual(len(set(TestLetterboxdFunctions.ids)), len(TestLetterboxdFunctions.ids))

    def test_get_ratings(self):
        self.assertIsInstance(TestLetterboxdFunctions.ratings, list)
        self.assertIsInstance(TestLetterboxdFunctions.ratings[0], int)
        self.assertEqual(len(set(TestLetterboxdFunctions.ratings)), 11)

    def test_get_urls(self):
        self.assertIsInstance(TestLetterboxdFunctions.urls, list)
        self.assertIsInstance(TestLetterboxdFunctions.urls[0], str)
        self.assertEqual(len(set(TestLetterboxdFunctions.urls)), len(TestLetterboxdFunctions.urls))

    def test_scrape_ids_ratings_and_urls(self):
        self.assertIsInstance(TestLetterboxdFunctions.ids_ratings_urls, dict)
        self.assertEqual(len(TestLetterboxdFunctions.ids), len(TestLetterboxdFunctions.ratings))
        self.assertEqual(len(TestLetterboxdFunctions.ids), len(TestLetterboxdFunctions.urls))

    def test_parse_film_urls_as_html(self):
        self.assertEqual(TestLetterboxdFunctions.film_htmls_none, None)
        self.assertEqual(len(TestLetterboxdFunctions.film_htmls_full), len(TestLetterboxdFunctions.urls))
        self.assertIsInstance(TestLetterboxdFunctions.film_htmls_full, list)
        self.assertIsInstance(TestLetterboxdFunctions.film_htmls_full[0], str)


if __name__ == "__main__":
    unittest.main()
