import requests
import os
from pyspark.sql.types import StructType, StructField, DoubleType

class GeoUtils:
    @staticmethod
    def get_coords_from_api(city, country):
        """Функція для динамічного мапінгу координат через OpenCage API"""
        api_key = os.environ.get("OPENCAGE_API_KEY")
        if not city or not country or not api_key: return None, None
        try:
            url = f"https://api.opencagedata.com/geocode/v1/json?q={city},{country}&key={api_key}"
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                res = response.json().get('results')
                if res:
                    geo = res[0]['geometry']
                    return float(geo['lat']), float(geo['lng'])
        except: pass
        return None, None

    @staticmethod
    def get_schema():
        return StructType([
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True)
        ])