import tempfile
import unittest
from etl import *


class TestSongDataImport(unittest.TestCase):
    sample_song_data = """
    {"num_songs": 1, "artist_id": "AR7G5I41187FB4CE6C", "artist_latitude": null, "artist_longitude": null, 
    "artist_location": "London, England", "artist_name": "Adam Ant", "song_id": "SONHOTT12A8C13493C", 
    "title": "Something Girls", "duration": 233.40363, "year": 1982}
    """
    song_table_data = ['SONHOTT12A8C13493C', 'Something Girls', 'AR7G5I41187FB4CE6C', 1982, 233.40363]

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_dir_path = self.tmp_dir.name

        self.tmp_file = tempfile.NamedTemporaryFile(dir=self.tmp_dir_path, suffix='.json')
        self.tmp_file_path = self.tmp_file.name

        with open(self.tmp_file_path, "w") as f:
            f.write(self.sample_song_data)

        self.tmp_non_json_file = tempfile.NamedTemporaryFile(dir=self.tmp_dir_path, suffix='.txt')

    def test_get_files(self):
        """ Only gets .json files """

        result = get_files(self.tmp_dir_path)
        self.assertEqual(result[0], self.tmp_file_path)

    def test_import_data(self):
        result = import_data(self.tmp_file_path)
        self.assertEqual(result, self.sample_song_data)

    def test_validate_data(self):
        result = import_data(self.tmp_file_path)
        self.assertEqual(result, self.sample_song_data)

    def test_extract_data(self):
        result = import_data(self.tmp_file_path)
        self.assertEqual(result, self.sample_song_data)

if __name__ == '__main__':
    unittest.main()