from dataclasses import dataclass
from collections import defaultdict
from utils.string_utils import get_capturing_groups, get_username, get_playlist_type, get_playlist_title, \
    get_number_of_pages


@dataclass
class Playlist:
    url: str

    def get_playlist_metadata(self) -> dict:
        """Returns a dictionary storing all metadata about the playlist"""
        groups = get_capturing_groups(url=self.url)
        metadata = defaultdict()
        metadata["url"] = self.url
        metadata["user"] = get_username(groups)
        metadata["type"] = get_playlist_type(groups)
        metadata["title"] = get_playlist_title(groups)
        metadata["number_of_pages"] = get_number_of_pages(url=self.url)
        return metadata
