import tempfile
import unittest
import pandas as pd
from etl import *


class TestSongDataImport(unittest.TestCase):
    sample_song_data = '{"num_songs": 1, "artist_id": "AR7G5I41187FB4CE6C", "artist_latitude": null, ' \
    '"artist_longitude": null, "artist_location": "London, England", "artist_name": "Adam Ant", ' \
    '"song_id": "SONHOTT12A8C13493C", "title": "Something Girls", "duration": 233.40363, "year": 1982} \n' \
    '{"num_songs": 1, "artist_id": "AR7G5I41187FB4CE6D", "artist_latitude": null, "artist_longitude": null, ' \
    '"artist_location": "London, England", "artist_name": "Betty Blue", "song_id": "SONHOTT12A8C13493D", ' \
    '"title": "Something Else", "duration": 233.4012, "year": 2019}'

    song_table_data = ['SONHOTT12A8C13493C', 'Something Girls', 'AR7G5I41187FB4CE6C', 1982, 233.40363]

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_dir_path = self.tmp_dir.name

        self.tmp_non_json_file = tempfile.NamedTemporaryFile(dir=self.tmp_dir_path, suffix='.txt')
        self.tmp_json_file = tempfile.NamedTemporaryFile(dir=self.tmp_dir_path, suffix='.json')
        self.tmp_file_path = self.tmp_json_file.name

        with open(self.tmp_file_path, "w") as f:
            f.write(self.sample_song_data)

    def test_extract_json_data_from_dir(self):
        expected_result = pd.DataFrame({
            'num_songs': [1, 1],
            'artist_id': ['AR7G5I41187FB4CE6C', 'AR7G5I41187FB4CE6D'],
            'artist_latitude': [None, None],
            'artist_longitude': [None, None],
            'artist_location': ['London, England', 'London, England'],
            'artist_name': ['Adam Ant', 'Betty Blue'],
            'song_id': ['SONHOTT12A8C13493C', 'SONHOTT12A8C13493D'],
            'title': ['Something Girls', 'Something Else'],
            'duration': [233.40363, 233.4012],
            'year': [1982, 2019]},
            range(0, 2)
        )

        result = extract_json_data_from_dir(self.tmp_dir_path)
        pd.testing.assert_frame_equal(expected_result, result)

    def test_get_files(self):
        """ Only gets .json files """

        result = get_files(self.tmp_dir_path)
        self.assertListEqual(result, [self.tmp_file_path])

    def test_extract_data_from_df(self):
        df1 = pd.DataFrame({
            'a': [1, 2],
            'b': [3, 4],
            'c': [5, 6]})

        result = extract_data_from_df(df1, ['a', 'c'])
        self.assertEqual([1, 5], result)


class TestSongPlayDataImport(unittest.TestCase):
    sample_log_data = """
    {'artist': 'Frumpies', 'auth': 'Logged In', 'firstName': 'Anabelle', 'gender': 'F', 'itemInSession': 0, 
    'lastName': 'Simpson', 'length': 134.47791, 'level': 'free', 
    'location': 'Philadelphia-Camden-Wilmington, PA-NJ-DE-MD', 'method': 'PUT', 'page': 'NextSong', 
    'registration': 1541044398796.0, 'sessionId': 455, 'song': 'Fuck Kitty', 'status': 200, 'ts': 1541903636796, 
    'userAgent': '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"', 
    'userId': '69'}
    """

    def test_filter_song_plays(self):
        df = pd.DataFrame({
            'test': [1, 2],
            'page': ["Home", "NextSong"]},
            ['a', 'b']
        )

        expected_result = pd.DataFrame({
            'test': [2],
            'page': ["NextSong"]},
            ['b']
        )

        result = filter_song_plays(df)
        pd.testing.assert_frame_equal(expected_result, result)

    def test_extract_time_data(df):
        df = pd.DataFrame({
            'ts': [1591017855401]
        })

        expected_result = pd.DataFrame({
            'start_time': [1591017855401],
            'hour': [13],
            'day': [1],
            'week': [23],
            'month': [6],
            'year': [2020],
            'weekday': [0]
        })

        result = extract_time_data(df)
        pd.testing.assert_frame_equal(expected_result, result)


if __name__ == '__main__':
    unittest.main()