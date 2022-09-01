import re
from utils.generic_scraping_functions import get_soup_object


def get_capturing_groups(url: str) -> list[str]:
    """Extract four capture groups from the playlist URL: the base URL, the username, the list type and the list
    title"""
    reg_exr = r"(https://letterboxd.com/)([a-zA-Z0-9]+)/([\bfilms\b]*[\blist\b])*/([a-zA-Z0-9-]+)"
    matches = re.search(pattern=reg_exr,
                        string=url)
    return [str(group) for group in matches.groups()]


def get_username(groups: list[str]) -> str:
    """Extract the username from the playlist URL"""
    # groups = get_capturing_groups(url=url)
    return groups[1].replace("-", "_")


def get_playlist_type(groups: list[str]) -> str:
    """Extract the playlist type from the playlist URL"""
    # groups = self.get_capturing_groups()
    return groups[2]


def get_playlist_title(groups: list[str]) -> str:
    """Extract the playlist title from the playlist URL"""
    # groups = self.get_capturing_groups()
    return groups[-1].replace("-", "_")


def get_number_of_pages(url: str) -> int:
    """Returns the total number of pages in a playlist by scraping it from the first page.
    Later, we're going to loop over all pages in the playlist. To do so we need to create a URL for
    each page in the playlist"""

    soup = get_soup_object(page=url, is_parsed_html=False)
    try:
        last_li = soup.find_all("li", class_="paginate-page")[-1]
        return int(last_li.text)
    except IndexError:
        return 1
