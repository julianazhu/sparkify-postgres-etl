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
        '"title": "Something Else", "duration": 233.4012, "year": 2019} \n'

    song_table_data = ['SONHOTT12A8C13493C', 'Something Girls', 'AR7G5I41187FB4CE6C', 1982, 233.40363]

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_dir_path = self.tmp_dir.name

        self.tmp_non_json_file = tempfile.NamedTemporaryFile(dir=self.tmp_dir_path, suffix='.txt')
        self.tmp_json_file = tempfile.NamedTemporaryFile(dir=self.tmp_dir_path, suffix='.json')
        self.tmp_json_file_path = self.tmp_json_file.name

        with open(self.tmp_json_file_path, "w") as f:
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
            'year': [1982, 2019]
        })

        result = extract_json_data_from_dir(self.tmp_dir_path)
        pd.testing.assert_frame_equal(expected_result, result)

    def test_validate_json(self):
        self.assertFalse(validate_json("[]s;]"))

    def test_get_files(self):
        """ Should only get the .json file """

        result = get_files(self.tmp_dir_path)
        self.assertListEqual(result, [self.tmp_json_file_path])


class TestSongPlayDataImport(unittest.TestCase):
    sample_log_data = """
    {'artist': 'Frumpies', 'auth': 'Logged In', 'firstName': 'Anabelle', 'gender': 'F', 'itemInSession': 0, 
    'lastName': 'Simpson', 'length': 134.47791, 'level': 'free', 
    'location': 'Philadelphia-Camden-Wilmington, PA-NJ-DE-MD', 'method': 'PUT', 'page': 'NextSong', 
    'registration': 1541044398796.0, 'sessionId': 455, 'song': 'Fuck Kitty', 'status': 200, 'ts': 1541903636796, 
    'userAgent': '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"', 
    'userId': '69'}
    """

    def test_filter_songplays(self):
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

        result = filter_songplays(df)
        pd.testing.assert_frame_equal(expected_result, result)

    def test_transform_time_data(df):
        df = pd.DataFrame({
            'ts': [1591017855401, 1591279264136]
        })

        expected_result = pd.DataFrame({
            'start_time': [1591017855401, 1591279264136],
            'hour': [13, 14],
            'day': [1, 4],
            'week': [23, 23],
            'month': [6, 6],
            'year': [2020, 2020],
            'weekday': [0, 3]
        })

        result = transform_time_data(df)
        pd.testing.assert_frame_equal(expected_result, result)


if __name__ == '__main__':
    unittest.main()