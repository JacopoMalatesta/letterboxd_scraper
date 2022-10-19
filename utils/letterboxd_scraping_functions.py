import re
import bs4
import itertools
from collections import defaultdict
from typing import Any
from dataclasses import dataclass
import numpy as np
import json
import logging
from utils.generic_scraping_functions import parse_url_synchronously, get_soup_object_out_of_parsed_html
from utils.time_utils import time_it
from utils.logging_utils import Logger

info_log = Logger(name=__name__, level=logging.INFO).return_logger()


def get_number_of_pages(url: str) -> int:
    """Returns the total number of pages in a playlist by scraping it from the first page.
    Later, we're going to loop over all pages in the playlist. To do so we need to create a URL for
    each page in the playlist"""

    html = parse_url_synchronously(url=url)
    soup = get_soup_object_out_of_parsed_html(html_page=html)
    try:
        last_li = soup.find_all("li", class_="paginate-page")[-1]
        return int(last_li.text)
    except IndexError:
        return 1


def get_url_for_each_page(metadata: dict) -> list[str]:
    """Returns the URL for all pages in the playlist"""
    url_with_page_number = metadata["url"] + "/page/{}/"
    return [url_with_page_number.format(i) for i in range(1, metadata["number_of_pages"] + 1)]


def unnest_list(lst: list[list[Any]]) -> list[Any]:
    """Unlists a nested list"""
    return list(itertools.chain(*lst))


def get_ids(soup: bs4.BeautifulSoup) -> list[int]:
    """Scrapes all film IDs from a playlist page"""
    films = soup.find_all("div", class_="really-lazy-load")
    return [int(film.get("data-film-id")) for film in films]


def get_ratings(soup: bs4.BeautifulSoup) -> list[int]:
    """Scrapes all ratings from a playlist page"""
    try:
        films = soup.find_all("li", class_="poster-container")
        return [int(film.get("data-owner-rating")) for film in films]
    except TypeError:
        spans = [str(span) for span in soup.find_all("span", class_=re.compile("rating -tiny -darker rated"))]
        return [int(re.findall(string=span, pattern=r"\d+")[0]) for span in spans]


def get_film_urls(soup: bs4.BeautifulSoup) -> list[str]:
    """Scrapes all film URLs from a playlist page"""
    films = soup.find_all("div", class_="really-lazy-load")
    return ["https://letterboxd.com" + film.get("data-film-slug") for film in films]


@time_it
def scrape_ids_ratings_and_urls(pages_soups: list[bs4.BeautifulSoup]) -> dict:
    """Scrapes all film IDs, ratings and urls from a playlist and stores them in a dictionary"""
    ids, ratings, film_urls = [], [], []

    for soup in pages_soups:
        id_list, rating_list, url_list = get_ids(soup), get_ratings(soup), get_film_urls(soup)
        ids.append(id_list)
        ratings.append(rating_list)
        film_urls.append(url_list)

    ids_ratings_urls_dict = defaultdict()
    ids_ratings_urls_dict["ids"] = unnest_list(ids)
    ids_ratings_urls_dict["ratings"] = unnest_list(ratings)
    ids_ratings_urls_dict["urls"] = unnest_list(film_urls)
    return ids_ratings_urls_dict


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
def scrape_remaining_film_data(film_soups: list[bs4.BeautifulSoup, Any]) -> dict or None:
    """Stores all IDs, titles, years, directors, actors and countries in a dictionary"""

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
