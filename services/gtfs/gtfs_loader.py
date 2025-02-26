# services/gtfs/gtfs_loader.py
import partridge as ptg
import datetime
import pandas as pd
from config import GTFS_DATA_PATH

# 시간 문자열을 초로 변환 (HH:MM 또는 HH:MM:SS)
def time_to_seconds(time_str):
    parts = time_str.split(':')
    if len(parts) == 2:
        h, m = map(int, parts)
        return h * 3600 + m * 60
    elif len(parts) == 3:
        h, m, s = map(int, parts)
        return h * 3600 + m * 60 + s
    raise ValueError("Time must be in HH:MM or HH:MM:SS format")

# 초를 "HH:MM" 형식의 문자열로 변환
def secs_to_hhmm(seconds):
    return (datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=seconds)).strftime("%H:%M")

class GTFSLoader:
    def __init__(self, gtfs_path=GTFS_DATA_PATH):
        self.gtfs_path = gtfs_path
        self.feed_data = None

    def load_feed(self):
        # GTFS 데이터 로드 및 시간값(초) 변환
        busiest_date, service_ids = ptg.read_busiest_date(self.gtfs_path)
        view = {'trips.txt': {'service_id': service_ids}}
        self.feed_data = ptg.load_feed(self.gtfs_path, view)
        self._convert_times()

    def _convert_times(self):
        # GTFS 테이블의 시간 데이터를 초 단위로 변환
        for col in ['departure_time', 'arrival_time']:
            if self.feed_data.stop_times[col].dtype == object:
                self.feed_data.stop_times[col] = self.feed_data.stop_times[col].apply(time_to_seconds)

    def get_feed_data(self):
        if self.feed_data is None:
            self.load_feed()
        return self.feed_data

def build_station_data(feed_data):
    # GTFS 테이블을 병합하여 정류장 메타데이터 생성
    df_stops = feed_data.stops[['stop_id', 'stop_name']]
    df_trips = feed_data.trips[['trip_id', 'route_id']]
    df_routes = feed_data.routes[['route_id', 'route_short_name', 'agency_id']]
    df_stop_times = feed_data.stop_times[['stop_id', 'trip_id']]

    merged_df = pd.merge(df_stop_times, df_trips, on='trip_id', how='left')
    merged_df = pd.merge(merged_df, df_routes, on='route_id', how='left')
    merged_df = pd.merge(merged_df, df_stops, on='stop_id', how='left')

    grouped = merged_df.groupby('stop_id', as_index=False).first()
    station_data = []
    for _, row in grouped.iterrows():
        station_data.append({
            'stop_id': row['stop_id'],
            'stop_name': row['stop_name'] if pd.notna(row['stop_name']) else 'Unknown',
            'operator': row['agency_id'] if pd.notna(row['agency_id']) else 'Unknown',
            'line': row['route_short_name'] if pd.notna(row['route_short_name']) else 'Unknown',
            'line_info': f"{row['route_short_name']}" if pd.notna(row['route_short_name']) else ''
        })
    return station_data

def create_gdf(feed_data):
    """
    정류장 위치 정보를 GeoDataFrame으로 생성 후 AEQD 좌표계로 변환
    """
    import geopandas as gpd
    import pyproj
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        {"stop_id": feed_data.stops.stop_id.tolist()},
        geometry=[Point(lon, lat) for lat, lon in zip(feed_data.stops.stop_lat, feed_data.stops.stop_lon)]
    )
    gdf = gdf.set_index("stop_id")
    gdf.crs = 'epsg:4326'
    centroid = gdf.iloc[0].geometry.centroid
    aeqd_crs = pyproj.CRS(
        proj='aeqd',
        ellps='WGS84',
        datum='WGS84',
        lat_0=centroid.y,
        lon_0=centroid.x
    )
    gdf = gdf.to_crs(crs=aeqd_crs)
    return gdf