import bs4
import requests
import aiohttp
import asyncio
import concurrent.futures
from enum import Enum


def get_soup_object(page: str, is_parsed_html: bool = True) -> bs4.BeautifulSoup:
    """Turns either a URL or an HTML page into a BeautifulSoup object"""

    if is_parsed_html:
        soup = bs4.BeautifulSoup(page, 'lxml')
    else:
        content = requests.get(page).content
        soup = bs4.BeautifulSoup(content, 'lxml')

    return soup


async def fetch_html_page(session, url: str) -> list[str]:
    """Asynchronously get the HTML code out of a URL"""
    async with session.get(url) as r:
        if r.status != 200:
            print(f"{r.status} for {url}")
        else:
            return await r.text()


async def fetch_all_html_pages(urls: list[str]):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_html_page(session, url) for url in urls]
        https = await asyncio.gather(*tasks)
        return https


def run_event_loop(urls: list[str]) -> list[str]:
    """Runs the event loop to get the HTML code for each url in the urls list"""
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(fetch_all_html_pages(urls))


def parse_urls_synchronously(urls: list[str]) -> list[str]:
    return [requests.get(url).content for url in urls]


class ParsingTechnique(Enum):
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"


def parse_urls(urls: list[str], technique: ParsingTechnique) -> list[str]:
    if technique == ParsingTechnique.ASYNCHRONOUS:
        return run_event_loop(urls=urls)
    elif technique == ParsingTechnique.SYNCHRONOUS:
        return parse_urls_synchronously(urls=urls)


class ParallelTechnique(Enum):
    MULTIPROCESSING = "multiprocessing"
    MULTITHREADING = "multithreading"
    SYNCHRONOUS = "synchronous"


def get_all_soup_objects(html_pages: list[str], technique: ParallelTechnique) -> list[bs4.BeautifulSoup]:
    """Turns HTML pages into BeautifulSoup objects with either multiprocessing or multithreading"""
    if technique == ParallelTechnique.MULTIPROCESSING:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            soups = executor.map(get_soup_object, html_pages)

        return list(soups)

    elif technique == ParallelTechnique.MULTITHREADING:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            soups = executor.map(get_soup_object, html_pages)

        return list(soups)

    elif technique == ParallelTechnique.SYNCHRONOUS:
        return [get_soup_object(page=html_page) for html_page in html_pages]
