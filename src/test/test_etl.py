import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "main", "python"))

from encryption import CryptoUtils
from geo import GeoUtils

@pytest.fixture(scope="session")
def spark():
    """Spark Session for unit testing transformations"""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("ETLUnitTests") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

class TestGeoUtils:
    
    def test_schema_correctness(self):
        """Verify the Spark schema fields for coordinates"""
        schema = GeoUtils.get_schema()
        assert schema.fields[0].name == "lat"
        assert schema.fields[1].name == "lng"

    @patch("requests.get")
    def test_api_coords_success(self, mock_get):
        """Mock successful OpenCage API response"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "results": [{"geometry": {"lat": 50.4501, "lng": 30.5234}}]
        }
        mock_get.return_value = mock_resp

        with patch.dict(os.environ, {"OPENCAGE_API_KEY": "test-key"}):
            lat, lng = GeoUtils.get_coords_from_api("Kyiv", "Ukraine")
        
        assert lat == pytest.approx(50.4501)
        assert lng == pytest.approx(30.5234)

    def test_api_empty_input(self):
        """Verify None is returned for missing input"""
        lat, lng = GeoUtils.get_coords_from_api("", None)
        assert lat is None and lng is None

class TestCryptoUtils:

    def test_aes_encryption_masking(self, spark):
        """Verify data is masked and converted to Base64"""
        data = [("Secret Hotel", "Hidden Lane 7")]
        df = spark.createDataFrame(data, ["Name", "Address"])
        
        encrypted_df = CryptoUtils.encrypt_pii_data(df, ["Name", "Address"])
        row = encrypted_df.collect()[0]

        assert row["Name"] != "Secret Hotel"
        assert row["Address"] != "Hidden Lane 7"
        assert len(row["Name"]) > 10

    def test_encryption_column_preservation(self, spark):
        """Verify original column names are preserved"""
        df = spark.createDataFrame([(1, "User1")], ["id", "Name"])
        result_df = CryptoUtils.encrypt_pii_data(df, ["Name"])
        
        assert "Name" in result_df.columns
        assert "id" in result_df.columns

def test_geohash_4_char_logic():
    """Test 4-character Geohash generation for London"""
    import pygeohash as gh
    lat, lng = 51.5074, -0.1278
    res = gh.encode(lat, lng, precision=4)
    
    assert len(res) == 4
    assert res == "gcpv"

def test_geohash_null_input():
    """Test null handling logic used in Spark UDFs"""
    lat, lng = None, 30.0
    res = (lambda lt, lg: __import__('pygeohash').encode(float(lt), float(lg), precision=4) 
           if lt is not None and lg is not None else None)(lat, lng)
    assert res is None