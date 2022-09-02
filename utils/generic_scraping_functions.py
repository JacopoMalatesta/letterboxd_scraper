import bs4
import requests
import aiohttp
import asyncio
import concurrent.futures
from utils.time_utils import time_it


def parse_url_synchronously(url: str) -> str:
    """Parse a URL string into HTML code in a synchronous way"""
    return requests.get(url).content


def get_soup_object_out_of_parsed_html(html_page: str) -> bs4.BeautifulSoup:
    """Returns a BeautifulSoup object out of a parsed HTML page"""
    return bs4.BeautifulSoup(html_page, 'lxml')


async def parse_url_asynchronously(session, url: str) -> list[str]:
    """Asynchronously gets the HTML code out of a URL"""
    async with session.get(url) as r:
        if r.status != 200:
            print(f"{r.status} for {url}")
        else:
            return await r.text()


async def event_loop(urls: list[str]):
    """Creates an event loop to parse a list of URLs as HTML code"""
    async with aiohttp.ClientSession() as session:
        tasks = [parse_url_asynchronously(session, url) for url in urls]
        return await asyncio.gather(*tasks)


@time_it
def parse_all_urls_asynchronously(urls: list[str]) -> list[str]:
    """Runs the event loop to get the HTML code for each url in the urls list"""
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(event_loop(urls))


@time_it
def get_soup_objects_multiprocessing(html_pages: list[str]) -> list[bs4.BeautifulSoup]:
    """Uses multiprocessing to get BeautifulSoup objects out of a list of HTML pages"""
    with concurrent.futures.ProcessPoolExecutor() as executor:
        soups = executor.map(get_soup_object_out_of_parsed_html, html_pages)

    return list(soups)


@time_it
def get_soup_objects_multithreading(html_pages: list[str]) -> list[bs4.BeautifulSoup]:
    """Uses multithreading to get BeautifulSoup objects out of a list of HTML pages"""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        soups = executor.map(get_soup_object_out_of_parsed_html, html_pages)

    return list(soups)


@time_it
def get_soup_objects_synchronously(html_pages: list[str]) -> list[bs4.BeautifulSoup]:
    """Synchronously gets BeautifulSoup objects out of a list of HTML pages"""
    return [get_soup_object_out_of_parsed_html(html_page=html_page) for html_page in html_pages]
