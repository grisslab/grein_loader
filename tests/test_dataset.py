import unittest
import logging
import requests
import grein_loader as loader

LOGGER = logging.getLogger(__name__)


class TestDataset(unittest.TestCase):
    geo_accession = "GSE112749"
    geo_accession_2 = "GSE100075"

    def test_connection_to_grein(self):
        LOGGER.info("Test GREIN connection")
        r = requests.get("http://www.ilincs.org/apps/grein/")
        LOGGER.info("GREIN Connection with status code: ", r.status_code)
        self.assertEqual(r.status_code, 200, "Error GREIN not available with http://www.ilincs.org/apps/grein/")

    def test_dataset(self):
        LOGGER.info("Test dataset with geo accession: ", self.geo_accession)
        r = requests.get("http://www.ilincs.org/apps/grein/?gse=GSE112749")
        LOGGER.warning(f"Grein dataset {self.geo_accession} with status code: { r.status_code }")
        self.assertEqual(r.status_code, 200, f"Error GREIN dataset {self.geo_accession} not available")

    def test_dataset_request(self):
        LOGGER.info(f"Test GREIN dataset with GeoId: {self.geo_accession}")
        description, metadata, count_matrix = loader.load_dataset(self.geo_accession)
        self.assertIsNotNone(description)
        self.assertIsNotNone(metadata)
        self.assertIsNotNone(count_matrix)

    def test_dataset_request_2(self):
        LOGGER.info(f"Test GREIN dataset with GeoId: {self.geo_accession_2}")
        description, metadata, count_matrix = loader.load_dataset(self.geo_accession_2)
        self.assertIsNotNone(description)
        self.assertIsNotNone(metadata)
        self.assertIsNotNone(count_matrix)

    def test_metadata_label(self):
        LOGGER.info(f"Test GREIN metadata with GeoId: {self.geo_accession}")
        description, metadata, count_matrix = loader.load_dataset(self.geo_accession)
        self.assertIsNotNone(metadata)
        self.assertIsNotNone(description)
        self.assertIsNotNone(count_matrix)

    def test_overview(self):
        LOGGER.info("Test overview of GREIN datasets")
        overview = loader.load_overview(10)
        self.assertEqual(10, len(overview))
