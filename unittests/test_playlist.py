import unittest
from utils.playlist_metadata import Playlist


class TestPlaylistMethods(unittest.TestCase):

    def setUp(self) -> None:
        self.marino_ratings = Playlist(url="https://letterboxd.com/souchousama/films/ratings/")
        self.chiara_2021 = Playlist(url="https://letterboxd.com/sonochiara123/list/2021/by/added-earliest/")
        self.jacopo_favs = Playlist(url="https://letterboxd.com/jacopomalatesta/list/favorite-feature-films/")
        self.jacopo_wishlist = Playlist(url="https://letterboxd.com/jacopomalatesta/list/short-term-blu-ray-watchlist/")

    def tearDown(self) -> None:
        pass

    def test_get_username(self) -> None:
        self.assertEqual(self.marino_ratings.get_username(), "souchousama")
        self.assertEqual(self.chiara_2021.get_username(), "sonochiara123")

    def test_get_playlist_type(self) -> None:
        self.assertEqual(self.marino_ratings.get_playlist_type(), "films")
        self.assertEqual(self.chiara_2021.get_playlist_type(), "list")

    def test_get_playlist_title(self) -> None:
        self.assertEqual(self.marino_ratings.get_playlist_title(), "ratings")
        self.assertEqual(self.chiara_2021.get_playlist_title(), "2021")

    def test_get_number_of_pages(self) -> None:
        self.assertEqual(self.marino_ratings.get_number_of_pages(), 178)
        self.assertEqual(self.chiara_2021.get_number_of_pages(), 1)
        self.assertEqual(self.jacopo_favs.get_number_of_pages(), 3)
        self.assertEqual(self.jacopo_wishlist.get_number_of_pages(), 2)


if __name__ == '__main__':
    unittest.main()
