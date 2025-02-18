import partridge as ptg
import geopandas as gpd
import pyproj
from shapely.geometry import Point
from copy import copy
from typing import Dict, Tuple, List, Any
import time
import pandas as pd

# GTFS 데이터 불러오기 (가장 혼잡한 날짜 기준)
path = 'ktdb_gtfs.zip'
_date, service_ids = ptg.read_busiest_date(path)
view = {'trips.txt': {'service_id': service_ids}}
feed = ptg.load_feed(path, view)

# 정류소 데이터를 GeoDataFrame으로 변환
gdf = gpd.GeoDataFrame(
    {"stop_id": feed.stops.stop_id.tolist()},
    geometry=[
        Point(lon, lat)
        for lat, lon in zip(feed.stops.stop_lat, feed.stops.stop_lon)
    ]
)
gdf = gdf.set_index("stop_id")
gdf.crs = 'epsg:5179'

# 거리 계산을 위해 미터 기반 좌표계로 변환
centroid = gdf.iloc[0].geometry.centroid
aeqd_crs = pyproj.CRS(
    proj='aeqd',
    ellps='GRS80', # 한국 TM에 맞게 설정
    lat_0=centroid.y,
    lon_0=centroid.x
)

gdf = gdf.to_crs(crs=aeqd_crs)

# 출발지와 도착지 설정
from_stop_name = "가천대"
to_stop_name = "장지"

# 출발지와 도착지의 정류소 ID 추출 (여러 개일 경우 첫 번째 행 선택)
from_stop = feed.stops[feed.stops.stop_name == from_stop_name].head(1).squeeze()
to_stop = feed.stops[feed.stops.stop_name.str.contains(to_stop_name)].head(1).squeeze()

from_stop_id = from_stop.stop_id
to_stop_id = to_stop.stop_id

# 출발 시간 (초 단위, 오전 8:30 기준)
departure_secs = 8.5 * 60 * 60


# 사전 인덱스 구축: 정류소별 stop_times 데이터
stop_times_by_stop: Dict[str, pd.DataFrame] = {}
for stop_id, group in feed.stop_times.groupby("stop_id"):
    stop_times_by_stop[stop_id] = group.sort_values("departure_time")


# 사전 인덱스 구축: 각 trip별 정류소 구간(segment)
trip_segments: Dict[str, pd.DataFrame] = {}
for trip_id, group in feed.stop_times.groupby("trip_id"):
    trip_segments[trip_id] = group.sort_values("stop_sequence").reset_index(drop=True)


# 공간 인덱스 생성 (보행 환승 최적화)
sindex = gdf.sindex


# 경로 탐색 함수 (버스/교통수단)
# 각 정류소에 도착한 최단 시간과 함께 경로 정보를 추적
# 경로 정보는 리스트의 형태로, 각 요소는 (정류소 ID, 이동 수단, 도착 시각) 튜플
# 출발지는 (from_stop_id, None, 0)으로 초기화
def stop_times_for_kth_trip(
    time_to_stops: Dict[str, float],
    paths: Dict[str, List[Tuple[Any, Any, float]]]
) -> Tuple[Dict[str, float], Dict[str, List[Tuple[Any, Any, float]]]]:
    new_times = copy(time_to_stops)
    new_paths = dict(paths)
    for ref_stop_id, baseline_cost in time_to_stops.items():
        current_time = departure_secs + baseline_cost
        candidate_rows = stop_times_by_stop.get(ref_stop_id)
        if candidate_rows is None:
            continue
        candidate_rows = candidate_rows[candidate_rows.departure_time >= current_time]
        for _, row in candidate_rows.iterrows():
            trip_id = row.trip_id
            seg = trip_segments[trip_id]
            try:
                start_idx = seg.index[seg['stop_id'] == ref_stop_id][0]
            except IndexError:
                continue
            seg_after = seg.loc[start_idx:]
            for _, row2 in seg_after.iterrows():
                arrival_time_adjusted = row2.arrival_time - departure_secs + baseline_cost
                stop_id = row2.stop_id
                if stop_id not in new_times or new_times[stop_id] > arrival_time_adjusted:
                    new_times[stop_id] = arrival_time_adjusted
                    # 경로 업데이트: 기존 경로에 (도착 정류소, trip_id, 도착 시각)을 추가
                    new_paths[stop_id] = paths[ref_stop_id] + [(stop_id, trip_id, arrival_time_adjusted)]
    return new_times, new_paths


# 경로 탐색 함수 (보행 환승)
def add_footpath_transfers(
    time_to_stops: Dict[str, float],
    paths: Dict[str, List[Tuple[Any, Any, float]]],
    transfer_cost=5*60
) -> Tuple[Dict[str, float], Dict[str, List[Tuple[Any, Any, float]]]]:
    new_times = copy(time_to_stops)
    new_paths = dict(paths)
    for stop_id, t in time_to_stops.items():
        stop_geom = gdf.loc[stop_id].geometry
        buffer_radius = 320  # 320미터 내
        buffer_geom = stop_geom.buffer(buffer_radius)
        possible_idx = list(sindex.intersection(buffer_geom.bounds))
        nearby_stops = gdf.iloc[possible_idx]
        for other_stop_id, row in nearby_stops.iterrows():
            if stop_geom.distance(row.geometry) <= buffer_radius:
                arrival_time_adjusted = t + transfer_cost
                if other_stop_id not in new_times or new_times[other_stop_id] > arrival_time_adjusted:
                    new_times[other_stop_id] = arrival_time_adjusted
                    # 보행 환승은 trip_id를 "foot"로 표기
                    new_paths[other_stop_id] = paths[stop_id] + [(other_stop_id, "foot", arrival_time_adjusted)]
    return new_times, new_paths


# RAPTOR 알고리즘 실행
# 초기 상태: 출발지에서의 소요 시간은 0, 경로는 출발지로만 구성
time_to_stops: Dict[str, float] = {from_stop_id: 0}
paths: Dict[str, List[Tuple[Any, Any, float]]] = {from_stop_id: [(from_stop_id, None, 0)]}
TRANSFER_LIMIT = 1  # 최대 환승 횟수

for k in range(TRANSFER_LIMIT + 1):
    print(f"\n{k}회 환승 가능한 경로 탐색 중...")
    current_stop_ids = list(time_to_stops.keys())
    print(f"\t현재 탐색 가능한 정류소 수: {len(current_stop_ids)}")

    # 1단계: 버스/교통수단 탐색
    tic = time.perf_counter()
    time_to_stops, paths = stop_times_for_kth_trip(time_to_stops, paths)
    toc = time.perf_counter()
    print(f"\t교통수단 탐색 완료 (소요 시간: {toc - tic:0.4f}초)")
    new_keys_count = len(time_to_stops) - len(current_stop_ids)
    print(f"\t\t새로 추가된 정류소 수: {new_keys_count}")

    # 2단계: 보행 환승 탐색
    tic = time.perf_counter()
    time_to_stops, paths = add_footpath_transfers(time_to_stops, paths)
    toc = time.perf_counter()
    print(f"\t보행 환승 추가 완료 (소요 시간: {toc - tic:0.4f}초)")
    new_keys_count = len(time_to_stops) - len(current_stop_ids)
    print(f"\t\t새로 추가된 정류소 수: {new_keys_count}")

# 도착지까지 경로 탐색 성공 여부 확인
assert to_stop_id in time_to_stops, "설정된 환승 횟수 내에서 도착지를 찾을 수 없습니다."

# 최종 경로 출력
final_path = paths[to_stop_id]
print(f"\n도착지까지 소요 시간: {time_to_stops[to_stop_id] / 60:.1f}분")
print("탐색된 경로 (정류소, 이동수단, 도착 시각(초)):")
for step in final_path:
    print(step)