import logging
import partridge as ptg
import geopandas as gpd
import pyproj
from shapely.geometry import Point
import time
import pandas as pd
import folium
from copy import copy
from typing import Any, Dict, List

# 로그 설정
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.debug("테스트 로그")

path = 'kr_gtfs.zip'
_date, service_ids = ptg.read_busiest_date(path)
view = {'trips.txt': {'service_id': service_ids}}
feed = ptg.load_feed(path, view)

gdf = gpd.GeoDataFrame(
    {"stop_id": feed.stops.stop_id.tolist()},
    geometry=[
        Point(lon, lat)
        for lat, lon in zip(feed.stops.stop_lat, feed.stops.stop_lon)
    ]
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

# 출발지와 도착지 정류장 설정
from_stop_name = "굴포천"
to_stop_name = "의왕(한국교통대)"

# 출발 시각 설정
departure_secs = 8.5 * 60 * 60

# 출발지와 도착지 정류장 정보 추출
from_stop = feed.stops[feed.stops.stop_name == from_stop_name].head(1).squeeze()
to_stop = feed.stops[feed.stops.stop_name == to_stop_name].head(1).squeeze()

# 정류장 ID 추출
from_stop_id = from_stop.stop_id
to_stop_id = to_stop.stop_id

print(f"출발 정류장: {from_stop}")
print(f"도착 정류장: {to_stop}")

# 주어진 정류장과 출발시간 이후의 모든 trip_id를 반환
def get_trip_ids_for_stop(feed, stop_id: str, departure_time: int) -> List[str]:
    mask_1 = feed.stop_times.stop_id == stop_id
    mask_2 = feed.stop_times.departure_time >= departure_time
    potential_trips = feed.stop_times[mask_1 & mask_2].trip_id.unique().tolist()
    return potential_trips

# 각 정류장까지 도달시간 정보를 바탕
# 출발 가능한 trip들을 탐색하여 도달시간 정보를 업데이트
def stop_times_for_kth_trip(
    from_stop_id: str,
    stop_ids: List[str],
    time_to_stops_orig: Dict[str, Any],
) -> Dict[str, Any]:
    time_to_stops = copy(time_to_stops_orig)
    stop_ids = list(stop_ids)
    potential_trips_num = 0

    for i, ref_stop_id in enumerate(stop_ids):
        baseline_cost, baseline_transfers = time_to_stops[ref_stop_id]

        potential_trips = get_trip_ids_for_stop(feed, ref_stop_id, departure_secs)
        potential_trips_num += len(potential_trips)

        for potential_trip in potential_trips:
            stop_times_sub = feed.stop_times[feed.stop_times.trip_id == potential_trip]
            stop_times_sub = stop_times_sub.sort_values(by="stop_sequence")

            from_here_subset = stop_times_sub[stop_times_sub.stop_id == ref_stop_id]
            from_here = from_here_subset.head(1).squeeze()

            stop_times_after_mask = stop_times_sub.stop_sequence >= from_here.stop_sequence
            stop_times_after = stop_times_sub[stop_times_after_mask]

            for arrive_time, arrive_stop_id in zip(stop_times_after.arrival_time, stop_times_after.stop_id):
                if ref_stop_id == from_stop_id and arrive_stop_id == from_stop_id:
                    arrive_time_adjusted = 0
                else:
                    arrive_time_adjusted = arrive_time - departure_secs + baseline_cost

                new_transfers = baseline_transfers + [arrive_stop_id]

                if arrive_stop_id in time_to_stops:
                    if time_to_stops[arrive_stop_id][0] > arrive_time_adjusted:
                        time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)
                else:
                    time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)

    print(f"최종 잠재 trip 개수: {potential_trips_num}")
    return time_to_stops

# 환승(도보 이동) 시 소요시간(3분)을 초 단위로 지정
TRANSFER_COST = 3 * 60

def add_footpath_transfers(
    stop_ids: List[str],
    time_to_stops_orig: Dict[str, Any],
    stops_gdf: gpd.GeoDataFrame,
    transfer_cost=TRANSFER_COST,
) -> Dict[str, Any]:
    time_to_stops = copy(time_to_stops_orig)
    stop_ids = list(stop_ids)

    for stop_id in stop_ids:
        stop_pt = stops_gdf.loc[stop_id].geometry
        meters_in_miles = 1610
        qual_area = stop_pt.buffer(meters_in_miles / 5)

        arrive_time_adjusted = time_to_stops[stop_id][0] + TRANSFER_COST
        new_transfers = time_to_stops[stop_id][1] + [stop_id]

        mask = stops_gdf.intersects(qual_area)
        for arrive_stop_id, _ in stops_gdf[mask].iterrows():
            if arrive_stop_id in time_to_stops:
                if time_to_stops[arrive_stop_id][0] > arrive_time_adjusted:
                    time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)
            else:
                time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)
    return time_to_stops

# 경로 탐색
time_to_stops = {from_stop_id: (0, [])}
TRANSFER_LIMIT = 1

for k in range(TRANSFER_LIMIT + 1):
    print(f"\n{k}회 환승 가능한 경로 탐색중")
    stop_ids = list(time_to_stops.keys())
    print(f"\t현재 탐색 가능한 정류장 개수: {len(stop_ids)}")

    tic = time.perf_counter()
    time_to_stops = stop_times_for_kth_trip(from_stop_id, stop_ids, time_to_stops)
    toc = time.perf_counter()
    print(f"\t정류장 도착시간 계산에 걸린 시간: {toc - tic:0.4f}초")
    added_keys_count = len(time_to_stops.keys()) - len(stop_ids)
    print(f"\t\t새로 추가된 정류장 수: {added_keys_count}")

    tic = time.perf_counter()
    stop_ids = list(time_to_stops.keys())
    time_to_stops = add_footpath_transfers(stop_ids, time_to_stops, gdf)
    toc = time.perf_counter()
    print(f"\t도보 환승 계산에 걸린 시간: {toc - tic:0.4f}초")
    added_keys_count = len(time_to_stops.keys()) - len(stop_ids)
    print(f"\t\t새로 추가된 정류장 수: {added_keys_count}")

# 도착 정류장까지의 경로가 존재하는지 확인
assert to_stop_id in time_to_stops, "환승 제한 내에 도착 경로를 찾지 못했습니다."

time_to_destination = time_to_stops[to_stop_id][0]
transfers = time_to_stops[to_stop_id][1]

print(f"도착 예상 소요 시간: {time_to_destination / 60:0.2f} 분")
print(f"환승 경로: {' -> '.join(transfers)}")

# 최종 경로 결과 출력 (테이블 형태)
transfers_info = []
transfers_info.append({'Stop ID': from_stop_id, 'Stop Name': from_stop_name, 'Arrival Time (분)': 0})
visited_stops = set([from_stop_id])

for stop_id in transfers:
    if stop_id == from_stop_id:
        continue
    arrival_time = time_to_stops[stop_id][0]
    stop_name = feed.stops.loc[feed.stops['stop_id'] == stop_id, 'stop_name'].iloc[0]
    transfers_info.append({'Stop ID': stop_id, 'Stop Name': stop_name, 'Arrival Time (분)': arrival_time / 60})
    visited_stops.add(stop_id)

df_transfers = pd.DataFrame(transfers_info)
print("최종 경로 정보:")
print(df_transfers)

def deduplicate_route(route: List[str]) -> List[str]:
    dedup = []
    for stop in route:
        if not dedup or dedup[-1] != stop:
            dedup.append(stop)
    return dedup

full_route = [from_stop_id] + transfers
full_route = deduplicate_route(full_route)
print("\n최종 정류장 경로 (정류장 ID):")
print(full_route)

# folium 지도 시각화
origin_lat = from_stop.stop_lat
origin_lon = from_stop.stop_lon
m = folium.Map(location=[origin_lat, origin_lon], zoom_start=13)

route_coords = []
for stop_id in full_route:
    stop_info = feed.stops[feed.stops['stop_id'] == stop_id].iloc[0]
    lat = stop_info.stop_lat
    lon = stop_info.stop_lon
    stop_name = stop_info.stop_name
    route_coords.append([lat, lon])
    folium.Marker(
        location=[lat, lon],
        popup=f"{stop_name} ({stop_id})",
        icon=folium.Icon(color="blue" if stop_id == to_stop_id else "green")
    ).add_to(m)

folium.PolyLine(route_coords, color="red", weight=3, opacity=0.8).add_to(m)
m.save("route_map.html")
print("\n경로 시각화 지도(route_map.html)가 생성되었습니다.")