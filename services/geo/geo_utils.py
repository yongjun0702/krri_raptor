#services/geo/geo_utils.py
import geopandas as gpd
import pyproj
from shapely.geometry import Point

class GeoData:
    def __init__(self, feed_stops):
        """
        feed_stops: DataFrame containing 'stop_id', 'stop_lat', 'stop_lon'
        """
        self.feed_stops = feed_stops
        self.gdf = None

    def create_geodataframe(self):
        # 정류장 위치 정보를 GeoDataFrame으로 생성
        self.gdf = gpd.GeoDataFrame(
            {"stop_id": self.feed_stops['stop_id'].tolist()},
            geometry=[Point(lon, lat) for lat, lon in zip(self.feed_stops['stop_lat'], self.feed_stops['stop_lon'])]
        )
        self.gdf = self.gdf.set_index("stop_id")
        self.gdf.crs = 'epsg:4326'
        return self.gdf

    def to_aeqd(self):
        # GeoDataFrame을 AEQD 좌표계로 변환
        if self.gdf is None:
            self.create_geodataframe()
        centroid = self.gdf.iloc[0].geometry.centroid
        aeqd_crs = pyproj.CRS(
            proj='aeqd',
            ellps='WGS84',
            datum='WGS84',
            lat_0=centroid.y,
            lon_0=centroid.x
        )
        self.gdf = self.gdf.to_crs(crs=aeqd_crs)
        return self.gdf