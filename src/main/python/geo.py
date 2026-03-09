"""
Module for geolocation utilities used in the Hotel Weather ETL pipeline.

Provides functionality to retrieve geographic coordinates (latitude/longitude)
from the OpenCage Geocoding API based on city and country names.

The API key is loaded from the environment variable OPENCAGE_API_KEY.
"""

import os
import logging
import requests
from pyspark.sql.types import StructType, StructField, DoubleType

logger = logging.getLogger(__name__)


class GeoUtils:
    """Utility class for geocoding city/country pairs via the OpenCage API.

    Uses the OPENCAGE_API_KEY environment variable for authentication.

    Example:
        >>> lat, lng = GeoUtils.get_coords_from_api("Kyiv", "Ukraine")
    """

    @staticmethod
    def get_coords_from_api(city, country):
        """Fetches latitude and longitude for a given city and country.

        Makes a request to the OpenCage Geocoding API and extracts
        coordinates from the first result.

        Args:
            city (str | None): Name of the city to geocode.
            country (str | None): Name of the country the city belongs to.

        Returns:
            tuple[float, float] | tuple[None, None]: A (latitude, longitude)
                tuple if the lookup succeeds, or (None, None) if the city/country
                is missing, the API key is not set, or the request fails.

        Example:
            >>> lat, lng = GeoUtils.get_coords_from_api("Lviv", "Ukraine")
            >>> print(lat, lng)  # 49.839683, 24.029717
        """
        api_key = os.environ.get("OPENCAGE_API_KEY")
        if not city or not country or not api_key:
            return None, None
        try:
            url = f"https://api.opencagedata.com/geocode/v1/json?q={city},{country}&key={api_key}"
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                res = response.json().get('results')
                if res:
                    geo = res[0]['geometry']
                    return float(geo['lat']), float(geo['lng'])
        except requests.RequestException as e:
            logger.warning("Geocoding failed for %s, %s: %s", city, country, e)
        return None, None

    @staticmethod
    def get_schema():
        """Returns the Spark schema for the geocoding UDF output.

        Returns:
            pyspark.sql.types.StructType: Schema with 'lat' and 'lng'
                fields, both DoubleType and nullable.

        Example:
            >>> schema = GeoUtils.get_schema()
        """
        return StructType([
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True)
        ])