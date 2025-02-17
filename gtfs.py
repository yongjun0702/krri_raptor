import partridge as ptg
import geopandas as gpd
import pyproj
from shapely.geometry import Point
from copy import copy
from typing import List, Dict, Any
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
    ])
gdf = gdf.set_index("stop_id")
gdf.crs = 'epsg:4326'

# 거리 계산을 위해 미터 기반 좌표계로 변환
aeqd = pyproj.Proj(
    proj='aeqd',
    ellps='WGS84',
    datum='WGS84',
    lat_0=gdf.iloc[0].geometry.centroid.y,
    lon_0=gdf.iloc[0].geometry.centroid.x).srs
gdf = gdf.to_crs(crs=aeqd)

# 출발지와 도착지 설정
from_stop_name = "굴포천역(신복사거리)"
to_stop_name = "한국철도기술연구원"

# 출발지와 도착지의 정류소 ID 추출
from_stop = feed.stops[feed.stops.stop_name == from_stop_name].iloc[0]
from_stop_id = from_stop['stop_id']

to_stop = feed.stops[feed.stops.stop_name.str.contains(to_stop_name)].iloc[0]
to_stop_id = to_stop['stop_id']

# 출발 시간 (초 단위, 오전 8:30 기준)
departure_secs = 8.5 * 60 * 60

########################################
# 사전 인덱스 구축: 정류소별 stop_times 데이터
########################################
# 각 정류소별로 stop_times를 그룹화하고 departure_time 기준으로 정렬
stop_times_by_stop: Dict[str, pd.DataFrame] = {}
for stop_id, group in feed.stop_times.groupby("stop_id"):
    stop_times_by_stop[stop_id] = group.sort_values("departure_time")

########################################
# 사전 인덱스 구축: 각 trip별 정류소 구간(segment)
########################################
trip_segments: Dict[str, pd.DataFrame] = {}
for trip_id, group in feed.stop_times.groupby("trip_id"):
    # stop_sequence 기준으로 정렬 후 인덱스를 초기화하여 순서에 따라 접근
    trip_segments[trip_id] = group.sort_values("stop_sequence").reset_index(drop=True)

########################################
# 공간 인덱스 생성 (보행 환승 최적화)
########################################
sindex = gdf.sindex

########################################
# 최적화된 버스 탐색 함수 (k번째 단계)
########################################
def stop_times_for_kth_trip(time_to_stops: Dict[str, float]) -> Dict[str, float]:
    """
    현재까지 도달한 각 정류소(time_to_stops)에 대해, 해당 정류소에서 출발 가능한 버스 구간을 탐색하여 도착 시간을 업데이트합니다.
    """
    new_times = copy(time_to_stops)
    for ref_stop_id, baseline_cost in time_to_stops.items():
        # 해당 정류소에 도착한 시각
        current_time = departure_secs + baseline_cost

        # 미리 그룹화한 데이터에서 해당 정류소의 stop_times를 가져오고, 현재 시간 이후의 노선을 필터링
        candidate_rows = stop_times_by_stop.get(ref_stop_id)
        if candidate_rows is None:
            continue
        candidate_rows = candidate_rows[candidate_rows.departure_time >= current_time]

        # 각 후보 노선(trip)에 대해
        for _, row in candidate_rows.iterrows():
            trip_id = row.trip_id
            seg = trip_segments[trip_id]
            # 해당 trip에서 ref_stop_id가 처음 등장하는 인덱스를 찾습니다.
            try:
                start_idx = seg.index[seg['stop_id'] == ref_stop_id][0]
            except IndexError:
                continue
            seg_after = seg.loc[start_idx:]
            # 해당 trip의 이후 정류소들을 순회하며 도착 시간 업데이트
            for _, row2 in seg_after.iterrows():
                # 도착 시간 조정: (해당 trip의 도착 시각 - 출발 기준 시각) + 기존 도착 비용
                arrival_time_adjusted = row2.arrival_time - departure_secs + baseline_cost
                stop_id = row2.stop_id
                if stop_id in new_times:
                    if new_times[stop_id] > arrival_time_adjusted:
                        new_times[stop_id] = arrival_time_adjusted
                else:
                    new_times[stop_id] = arrival_time_adjusted
    return new_times

########################################
# 최적화된 보행 환승 함수
########################################
TRANSFER_COST = 5 * 60  # 보행 환승 시간: 5분

def add_footpath_transfers(time_to_stops: Dict[str, float], transfer_cost=TRANSFER_COST) -> Dict[str, float]:
    """
    현재까지 도달한 정류소에 대해, 보행으로 환승 가능한 주변 정류소들을 spatial index를 사용해 탐색합니다.
    """
    new_times = copy(time_to_stops)
    for stop_id, t in time_to_stops.items():
        stop_geom = gdf.loc[stop_id].geometry
        # 0.2 마일 ≒ 320미터 내의 정류소 검색 (버퍼의 bounds를 사용하여 후보군을 빠르게 조회)
        buffer_radius = 320  # 미터 단위
        buffer_geom = stop_geom.buffer(buffer_radius)
        possible_idx = list(sindex.intersection(buffer_geom.bounds))
        nearby_stops = gdf.iloc[possible_idx]
        for other_stop_id, row in nearby_stops.iterrows():
            # 실제 거리를 계산하여 필터링 (버퍼 영역 내부 확인)
            if stop_geom.distance(row.geometry) <= buffer_radius:
                arrival_time_adjusted = t + transfer_cost
                if other_stop_id in new_times:
                    if new_times[other_stop_id] > arrival_time_adjusted:
                        new_times[other_stop_id] = arrival_time_adjusted
                else:
                    new_times[other_stop_id] = arrival_time_adjusted
    return new_times

########################################
# RAPTOR 알고리즘 실행
########################################
time_to_stops: Dict[str, float] = {from_stop_id: 0}  # 출발지 초기화
TRANSFER_LIMIT = 1  # 최대 환승 횟수

for k in range(TRANSFER_LIMIT + 1):
    print(f"\n{k}회 환승 가능한 경로 탐색 중...")
    current_stop_ids = list(time_to_stops.keys())
    print(f"\t현재 탐색 가능한 정류소 수: {len(current_stop_ids)}")

    # 1단계: 버스/교통수단 탐색
    tic = time.perf_counter()
    time_to_stops = stop_times_for_kth_trip(time_to_stops)
    toc = time.perf_counter()
    print(f"\t교통수단 탐색 완료 (소요 시간: {toc - tic:0.4f}초)")
    new_keys_count = len(time_to_stops) - len(current_stop_ids)
    print(f"\t\t새로 추가된 정류소 수: {new_keys_count}")

    # 2단계: 보행 환승 추가
    tic = time.perf_counter()
    time_to_stops = add_footpath_transfers(time_to_stops, TRANSFER_COST)
    toc = time.perf_counter()
    print(f"\t보행 환승 추가 완료 (소요 시간: {toc - tic:0.4f}초)")
    new_keys_count = len(time_to_stops) - len(current_stop_ids)
    print(f"\t\t새로 추가된 정류소 수: {new_keys_count}")

# 도착지까지 경로 탐색 성공 여부 확인
assert to_stop_id in time_to_stops, "설정된 환승 횟수 내에서 도착지를 찾을 수 없습니다."
print(f"도착지까지 소요 시간: {time_to_stops[to_stop_id] / 60:.1f}분")