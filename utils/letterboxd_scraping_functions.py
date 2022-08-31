import re
import bs4
import itertools
from collections import defaultdict
from typing import Any
import pandas as pd
from dataclasses import dataclass
import numpy as np
import json
import logging
from utils.generic_scraping_functions import get_all_soup_objects, parse_urls
from utils.time_utils import time_it
from utils.logging_utils import Logger
from utils.generic_scraping_functions import ParallelTechnique

info_log = Logger(name=__name__, level=logging.INFO).return_logger()


def get_url_for_each_page(playlist_url: str, number_of_pages: int) -> list[str]:
    """Returns the URL for all pages in the playlist"""
    url_with_page_number = playlist_url + "/page/{}/"
    return [url_with_page_number.format(i) for i in range(1, number_of_pages + 1)]


@time_it
def parse_playlist_pages_as_html(pages_urls: list[str], asynchronous: str) -> list[str]:
    """Asynchronously parses all pages in a playlist"""
    return parse_urls(urls=pages_urls, asynchronous=asynchronous)


@time_it
def get_soup_objects_from_playlist_pages(html_pages: list[str],
                                         technique: ParallelTechnique) -> list[bs4.BeautifulSoup]:
    """Get BeautifulSoup objects out of the HTML pages"""
    return get_all_soup_objects(html_pages=html_pages, technique=technique)


def unnest_list(lst: list[list[Any]]) -> list[Any]:
    """Unlists a nested list"""
    return list(itertools.chain(*lst))


def get_ids(soup: bs4.BeautifulSoup) -> list[int]:
    """Scrapes all film IDs from a playlist page"""
    films = soup.find_all("div", class_="really-lazy-load")
    return [int(film.get("data-film-id")) for film in films]


def get_ratings(soup: bs4.BeautifulSoup, playlist_type: str) -> list[int]:
    """Scrapes all ratings from a playlist page"""
    if playlist_type == "list":
        films = soup.find_all("li", class_="poster-container")
        return [int(film.get("data-owner-rating")) for film in films]
    elif playlist_type == "films":
        rated = [span.get("class")[1] for span in soup.find_all("span", class_="rating")]
        return [int(re.findall(r'\d+', r)[0]) for r in rated]


def get_film_urls(soup: bs4.BeautifulSoup) -> list[str]:
    """Scrapes all film URLs from a playlist page"""
    films = soup.find_all("div", class_="really-lazy-load")
    return ["https://letterboxd.com" + film.get("data-film-slug") for film in films]


@time_it
def scrape_ids_ratings_and_urls(pages_soups: list[bs4.BeautifulSoup], playlist_type: str) -> dict:
    """Scrapes all film IDs, ratings and urls from a playlist and stores them in a dictionary"""
    ids, ratings, film_urls = [], [], []

    for soup in pages_soups:
        id_list, rating_list, url_list = get_ids(soup), get_ratings(soup, playlist_type), get_film_urls(soup)
        ids.append(id_list)
        ratings.append(rating_list)
        film_urls.append(url_list)

    ids_ratings_urls = defaultdict()
    ids_ratings_urls["ids"] = unnest_list(ids)
    ids_ratings_urls["ratings"] = unnest_list(ratings)
    ids_ratings_urls["urls"] = unnest_list(film_urls)
    return ids_ratings_urls


@time_it
def parse_film_urls_as_html(current_df: pd.DataFrame or None,
                            new_records: list[str] or None,
                            film_urls: list[str],
                            asynchronous: str) -> list[str]:
    if current_df is not None and new_records:
        return parse_urls(urls=new_records, asynchronous=asynchronous)
    elif current_df is None:
        return parse_urls(urls=film_urls, asynchronous=asynchronous)


@time_it
def get_soup_objects_from_film_pages(current_df: pd.DataFrame,
                                     new_records: list[str],
                                     html_pages: list[str],
                                     technique: ParallelTechnique) -> list[bs4.BeautifulSoup]:
    if current_df is None or new_records:
        return get_all_soup_objects(html_pages=html_pages, technique=technique)


@dataclass
class FilmSoup:
    soup: bs4.BeautifulSoup

    def get_id(self) -> int:
        """Scrapes the film ID"""
        return int(self.soup.find("div", class_="really-lazy-load").get("data-film-id"))

    def get_title(self) -> str:
        s = self.soup.find("script", {"type": "application/ld+json"}).string
        s = s.replace('\n/* <![CDATA[ */\n', '').replace('\n/* ]]> */\n', '')
        d = json.loads(s)
        return d['name']

    def get_year(self) -> int or np.nan:
        """Scrapes the year"""
        try:
            s = self.soup.find("script", {"type": "application/ld+json"}).string
            s = s.replace('\n/* <![CDATA[ */\n', '').replace('\n/* ]]> */\n', '')
            d = json.loads(s)
            return int(d['releasedEvent'][0]['startDate'])
        except KeyError:
            return np.nan

    def get_director(self) -> str or np.nan:
        """Scrapes the director"""
        try:
            s = self.soup.find("script", {"type": "application/ld+json"}).string
            s = s.replace('\n/* <![CDATA[ */\n', '').replace('\n/* ]]> */\n', '')
            d = json.loads(s)
            names = [director['name'] for director in d['director']]
            return ';'.join(names)
        except KeyError:
            return np.nan

    def get_cast(self) -> str or np.nan:
        """Scrapes the cast"""
        try:
            s = self.soup.find("script", {"type": "application/ld+json"}).string
            s = s.replace('\n/* <![CDATA[ */\n', '').replace('\n/* ]]> */\n', '')
            d = json.loads(s)
            actors = [actor['name'] for actor in d['actors']]
            return ';'.join(actors)
        except KeyError:
            return np.nan

    def get_country(self) -> str or np.nan:
        """Scrapes the countries"""
        try:
            s = self.soup.find("script", {"type": "application/ld+json"}).string
            s = s.replace('\n/* <![CDATA[ */\n', '').replace('\n/* ]]> */\n', '')
            d = json.loads(s)
            countries_of_origin = [country['name'] for country in d['countryOfOrigin']]
            return ';'.join(countries_of_origin)
        except KeyError:
            return np.nan


@time_it
def scrape_remaining_film_data(current_df: pd.DataFrame,
                               new_records: list[str],
                               film_soups: list[bs4.BeautifulSoup, Any]) -> dict or None:
    """Stores all IDs, titles, years, directors, actors and countries in a dictionary"""

    if current_df is None or new_records:
        film_ids, titles, years, directors, actors, countries = [], [], [], [], [], []
        for film_soup in film_soups:
            film = FilmSoup(film_soup)

            film_id = film.get_id()
            title = film.get_title()
            year = film.get_year()
            director = film.get_director()
            cast = film.get_cast()
            country = film.get_country()

            film_ids.append(film_id)
            titles.append(title)
            years.append(year)
            directors.append(director)
            actors.append(cast)
            countries.append(country)

        remaining_film_data = defaultdict()
        remaining_film_data["film_ids"] = film_ids
        remaining_film_data["titles"] = titles
        remaining_film_data["years"] = years
        remaining_film_data["directors"] = directors
        remaining_film_data["actors"] = actors
        remaining_film_data["countries"] = countries

        return remaining_film_data
