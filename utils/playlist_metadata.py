import re
from dataclasses import dataclass
from collections import defaultdict
from utils.generic_scraping_functions import get_soup_object


@dataclass
class Playlist:
    url: str

    def get_playlist_metadata(self) -> dict:
        """Returns a dictionary storing all metadata about the playlist"""
        metadata = defaultdict()
        metadata["url"] = self.url
        metadata["user"] = self.get_username()
        metadata["type"] = self.get_playlist_type()
        metadata["title"] = self.get_playlist_title()
        metadata["number_of_pages"] = self.get_number_of_pages()
        return metadata

    def get_capturing_groups(self) -> list[str]:
        """Extract four capture groups from the playlist URL: the base URL, the username, the list type and the list
        title"""
        reg_exr = r"(https://letterboxd.com/)([a-zA-Z0-9]+)/([\bfilms\b]*[\blist\b])*/([a-zA-Z0-9-]+)"
        matches = re.search(pattern=reg_exr,
                            string=self.url)
        return [str(group) for group in matches.groups()]

    def get_username(self) -> str:
        """Extract the username from the playlist URL"""
        groups = self.get_capturing_groups()
        return groups[1].replace("-", "_")

    def get_playlist_type(self) -> str:
        """Extract the playlist type from the playlist URL"""
        groups = self.get_capturing_groups()
        return groups[2]

    def get_playlist_title(self) -> str:
        """Extract the playlist title from the playlist URL"""
        groups = self.get_capturing_groups()
        return groups[-1].replace("-", "_")

    def get_number_of_pages(self) -> int:
        """Returns the total number of pages in a playlist by scraping it from the first page.
        Later, we're going to loop over all pages in the playlist. To do so we need to create a URL for
        each page in the playlist"""

        soup = get_soup_object(page=self.url, is_parsed_html=False)
        try:
            last_li = soup.find_all("li", class_="paginate-page")[-1]
            return int(last_li.text)
        except IndexError:
            return 1
