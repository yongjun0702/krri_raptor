import partridge as ptg
import geopandas as gpd
import pyproj
from shapely.geometry import Point
import time
import pandas as pd
from collections import defaultdict
import datetime
import math
import numpy as np

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

# GTFS 데이터 로드 및 시간값(초) 변환
def load_gtfs(gtfs_path='ktdb_gtfs.zip'):
    """
    GTFS zip 파일을 읽어서 Partridge Feed 객체로 로드한 뒤,
    stop_times의 departure_time, arrival_time을 초 단위로 변환
    """
    busiest_date, service_ids = ptg.read_busiest_date(gtfs_path)
    view = {'trips.txt': {'service_id': service_ids}}
    feed_data = ptg.load_feed(gtfs_path, view)
    for col in ['departure_time', 'arrival_time']:
        if feed_data.stop_times[col].dtype == object:
            feed_data.stop_times[col] = feed_data.stop_times[col].apply(time_to_seconds)
    return feed_data

# PartialFeed 클래스: 원본 feed와 동일한 속성(stops, stop_times, trips, routes)을 제공
class PartialFeed:
    def __init__(self, stops, stop_times, trips, routes):
        self.stops = stops
        self.stop_times = stop_times
        self.trips = trips
        self.routes = routes

# 서울 내 데이터만 필터링하여 PartialFeed 객체로 반환
def filter_seoul_feed(feed):
    """
    서울 지역 (대략적인 위도, 경도 범위)만 필터링하여 PartialFeed로 반환
    """
    min_lat, max_lat = 37.4, 37.7
    min_lon, max_lon = 126.8, 127.1

    seoul_stops = feed.stops[
        (feed.stops['stop_lat'] >= min_lat) & (feed.stops['stop_lat'] <= max_lat) &
        (feed.stops['stop_lon'] >= min_lon) & (feed.stops['stop_lon'] <= max_lon)
    ]
    seoul_stop_ids = set(seoul_stops['stop_id'])
    seoul_stop_times = feed.stop_times[feed.stop_times['stop_id'].isin(seoul_stop_ids)]
    seoul_trip_ids = set(seoul_stop_times['trip_id'])
    seoul_trips = feed.trips[feed.trips['trip_id'].isin(seoul_trip_ids)]
    seoul_route_ids = set(seoul_trips['route_id'])
    seoul_routes = feed.routes[feed.routes['route_id'].isin(seoul_route_ids)]
    return PartialFeed(seoul_stops, seoul_stop_times, seoul_trips, seoul_routes)

# 정류장 위치 정보를 GeoDataFrame으로 생성 후, AEQD 좌표계로 변환
def create_gdf(feed_data):
    """
    정류장 위치 정보를 GeoDataFrame으로 생성 후, AEQD 좌표계로 변환
    - feed_data.stops에 stop_lat, stop_lon 존재
    """
    gdf = gpd.GeoDataFrame(
        {"stop_id": feed_data.stops.stop_id.tolist()},
        geometry=[Point(lon, lat) for lat, lon in zip(feed_data.stops.stop_lat, feed_data.stops.stop_lon)]
    )
    gdf = gdf.set_index("stop_id")
    gdf.crs = 'epsg:4326'
    centroid = gdf.iloc[0].geometry.centroid  # 첫 정류장을 기준으로 AEQD 투영
    aeqd_crs = pyproj.CRS(
        proj='aeqd',
        ellps='WGS84',
        datum='WGS84',
        lat_0=centroid.y,
        lon_0=centroid.x
    )
    gdf = gdf.to_crs(crs=aeqd_crs)
    return gdf

# GTFS 테이블을 병합하여 정류장 정보를 생성
def build_station_data(feed_data):
    """
    GTFS의 stops, trips, routes, stop_times 테이블을 병합하여
    정류장(역) 정보(stop_id, stop_name, operator, line 등)를 생성
    """
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

def raptor_search(feed_data, geo_data, from_stop_id, departure_secs, max_transfers):
    walking_speed = 1.4  # 도보 속도 (m/s)
    INF = math.inf

    # stop_id별 stop_times를 그룹화하여 딕셔너리화
    stop_groups = {sid: grp for sid, grp in feed_data.stop_times.groupby('stop_id')}
    # trip_id별로 stop_sequence 기준으로 정렬된 시간표 생성
    trip_groups = {tid: grp.sort_values('stop_sequence') for tid, grp in feed_data.stop_times.groupby('trip_id')}
    # 각 trip의 시간표를 NumPy 배열로 변환해 캐싱 (벡터화 및 캐싱)
    trip_cache = {}

    # 도보로 이동 가능한 인접 정류장을 계산 (320m 이내)
    spatial_index = geo_data.sindex
    radius = 320.0
    foot_paths = defaultdict(list)
    for station_id, row in geo_data.iterrows():
        station_buffer = row.geometry.buffer(radius)
        for idx in spatial_index.query(station_buffer, predicate="intersects"):
            neighbor_id = geo_data.index[idx]
            if neighbor_id != station_id:
                dist = row.geometry.distance(geo_data.loc[neighbor_id].geometry)
                if dist <= radius:
                    foot_paths[station_id].append((neighbor_id, dist / walking_speed if dist > 0 else 0))

    all_stops = feed_data.stops['stop_id'].unique()
    # 각 라운드별로 정류장 도착시간 저장 (초 단위)
    arrivals = [dict.fromkeys(all_stops, INF) for _ in range(max_transfers + 1)]
    # 각 라운드별 부모 정보 저장 (경로 추적용)
    parents = [dict.fromkeys(all_stops, None) for _ in range(max_transfers + 1)]
    # 각 라운드별 업데이트된 정류장 집합
    updated_stops = [set() for _ in range(max_transfers + 1)]

    arrivals[0][from_stop_id] = departure_secs
    updated_stops[0].add(from_stop_id)
    rounds_stats = []
    time_limit = 10800  # 3시간

    # 라운드 반복: (1) 노선 확장 후 (2) 도보 확장 적용 (노선 확장 결과는 다음 환승 라운드로 기록)
    for round_idx in range(max_transfers + 1):
        t_start = time.time()
        foot_updates = 0
        route_updates = 0

        # 노선 확장: 현재 정류장에서 열차/버스 이용으로 이동 가능한 정류장 업데이트
        if round_idx < max_transfers:
            newly_updated_next = set()
            for station_id in updated_stops[round_idx]:
                t_base = arrivals[round_idx][station_id]
                if t_base == INF or station_id not in stop_groups:
                    continue
                candidates = stop_groups[station_id]
                # 시간제한 필터 적용
                filtered = candidates[
                    (candidates['departure_time'] >= t_base) &
                    (candidates['departure_time'] <= t_base + time_limit)
                ]
                if filtered.empty:
                    continue
                # 이미 정렬되어 있으므로 이진 탐색으로 시작 인덱스 찾기
                sorted_candidates = filtered.sort_values('departure_time').reset_index(drop=True)
                dep_times = sorted_candidates['departure_time'].values
                start_idx = np.searchsorted(dep_times, t_base)
                valid_candidates = sorted_candidates.iloc[start_idx:]
                # 같은 trip_id별 가장 빠른 departure_time 행 선택
                best_rows = valid_candidates.groupby('trip_id', as_index=False).first()
                for _, best_row in best_rows.iterrows():
                    trip_id = best_row['trip_id']
                    origin_dep = best_row['departure_time']
                    origin_seq = best_row['stop_sequence']
                    wait_time = origin_dep - t_base  # 대기 시간
                    if wait_time < 0:
                        continue
                    # 캐싱: 해당 trip의 시간표를 NumPy 배열로 변환하여 재사용
                    if trip_id in trip_cache:
                        seq_arr, arr_arr, stopid_arr = trip_cache[trip_id]
                    else:
                        trip_df = trip_groups[trip_id]
                        seq_arr = trip_df['stop_sequence'].to_numpy()
                        arr_arr = trip_df['arrival_time'].to_numpy()
                        stopid_arr = trip_df['stop_id'].to_numpy()
                        trip_cache[trip_id] = (seq_arr, arr_arr, stopid_arr)
                    start_pos = np.searchsorted(seq_arr, origin_seq)
                    for i in range(start_pos, len(seq_arr)):
                        dest_id = stopid_arr[i]
                        candidate_arrival = arr_arr[i]
                        if candidate_arrival < arrivals[round_idx + 1][dest_id]:
                            arrivals[round_idx + 1][dest_id] = candidate_arrival
                            # 부모 정보: (이전 정류장, 현재 라운드, 'trip:열차ID', 출발 시각, 도착 시각, 대기 시간)
                            parents[round_idx + 1][dest_id] = (
                                station_id,
                                round_idx,
                                f"trip:{trip_id}",
                                origin_dep,
                                candidate_arrival,
                                wait_time
                            )
                            newly_updated_next.add(dest_id)
                            route_updates += 1
            updated_stops[round_idx + 1] = newly_updated_next

        # 도보 확장: 노선 확장 결과로 갱신된 정류장에 대해 도보 이동 추가 업데이트 (같은 라운드 내)
        if round_idx < max_transfers:
            newly_updated_foot = set()
            for stop_id in updated_stops[round_idx + 1]:
                base_time = arrivals[round_idx + 1][stop_id]
                if base_time == INF:
                    continue
                for nbr_id, foot_time in foot_paths.get(stop_id, []):
                    t_new = base_time + foot_time
                    if t_new < arrivals[round_idx + 1][nbr_id]:
                        arrivals[round_idx + 1][nbr_id] = t_new
                        parents[round_idx + 1][nbr_id] = (
                            stop_id,
                            round_idx + 1,
                            'foot',
                            base_time,
                            t_new,
                            foot_time
                        )
                        newly_updated_foot.add(nbr_id)
                        foot_updates += 1
            updated_stops[round_idx + 1].update(newly_updated_foot)

        t_elapsed = time.time() - t_start
        reached = sum(1 for s in all_stops if arrivals[round_idx][s] < INF)
        rounds_stats.append({
            '라운드': round_idx,
            '탐색 정류장 수': reached,
            '도보 업데이트': foot_updates,
            '노선 업데이트': route_updates,
            '소요시간(초)': t_elapsed
        })
        print(f"Round {round_idx}: 정류장 {reached}개, 도보 {foot_updates}건, 노선 {route_updates}건, 소요: {t_elapsed:.2f}초")
        if round_idx < max_transfers and len(updated_stops[round_idx + 1]) == 0:
            print(f"Round {round_idx + 1}: 업데이트 없음 -> 종료")
            break

    # 결과 복원: 각 정류장에 대해 가장 빠른 도착 시간을 찾아 역으로 경로 추적
    final_result = {}
    for station_id in all_stops:
        best_arrival_time = INF
        best_round = None
        for rr in range(max_transfers + 1):
            if arrivals[rr][station_id] < best_arrival_time:
                best_arrival_time = arrivals[rr][station_id]
                best_round = rr
        if best_arrival_time == INF:
            continue
        total_time = best_arrival_time - departure_secs  # 전체 소요 시간
        path_stops = []
        schedule_data = []
        cur_stop, cur_round = station_id, best_round
        visited_set = set()
        # 역 추적 (역 순서를 역으로 추적하여 경로 구성)
        while True:
            key = (cur_stop, cur_round)
            if key in visited_set:
                break
            visited_set.add(key)
            path_stops.append(cur_stop)
            par = parents[cur_round][cur_stop]
            if par is None:
                base_time = arrivals[0].get(cur_stop, departure_secs)
                schedule_data.append((cur_stop, secs_to_hhmm(base_time), secs_to_hhmm(base_time), 0, None))
                break
            # schedule_data: (정류장, 도착 시각, 열차 출발 시각 또는 도보 시작 시각, 소요 시간, 이동 모드)
            schedule_data.append((cur_stop, secs_to_hhmm(arrivals[cur_round][cur_stop]), secs_to_hhmm(par[3]), par[5], par[2]))
            cur_stop, cur_round = par[0], par[1]
        path_stops.reverse()
        schedule_data.reverse()
        final_result[station_id] = (total_time, path_stops, schedule_data)

    return final_result, arrivals, parents, rounds_stats, all_stops, INF

# 테스트 메인 함수
if __name__ == '__main__':
    gtfs_path = 'ktdb_gtfs.zip'
    feed_data = load_gtfs(gtfs_path)
    # 서울 데이터만 사용하고 싶으면 True, 아니면 False
    USE_SEOUL = True
    if USE_SEOUL:
        feed_data = filter_seoul_feed(feed_data)
    geo_data = create_gdf(feed_data)
    station_data = build_station_data(feed_data)

    from_stop = input("출발 정류장 ID를 입력하세요: ").strip()
    to_stop = input("도착 정류장 ID를 입력하세요: ").strip()
    dep_time_str = input("출발 시간을 HH:MM 또는 HH:MM:SS 형식으로 입력하세요: ").strip()
    dep_secs = time_to_seconds(dep_time_str)
    max_transfers = 3

    final_result, arrivals, parents, rounds_stats, all_stops, INF = raptor_search(
        feed_data, geo_data, from_stop, dep_secs, max_transfers
    )

    if to_stop in final_result:
        tot_time, route_stops, schedule_info = final_result[to_stop]
        print(f"총 소요 시간: {tot_time}초 ({secs_to_hhmm(tot_time)})")
        print("경로:", " -> ".join(route_stops))
        print("스케줄 정보:")
        for info in schedule_info:
            print(info)
    else:
        print("경로를 찾지 못했습니다.")