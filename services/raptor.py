# server/raptor.py
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
def load_gtfs(gtfs_path='kr_subway_gtfs.zip'):
    busiest_date, service_ids = ptg.read_busiest_date(gtfs_path)
    view = {'trips.txt': {'service_id': service_ids}}
    feed_data = ptg.load_feed(gtfs_path, view)
    for col in ['departure_time', 'arrival_time']:
        if feed_data.stop_times[col].dtype == object:
            feed_data.stop_times[col] = feed_data.stop_times[col].apply(time_to_seconds)
    return feed_data

# 정류장 위치 정보를 GeoDataFrame으로 생성 후, AEQD 좌표계로 변환
def create_gdf(feed_data):
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

# GTFS 테이블을 병합하여 정류장 정보를 생성
def build_station_data(feed_data):
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

# RAPTOR 알고리즘
def raptor_search(feed_data, geo_data, from_stop_id, departure_secs, max_transfers):
    walking_speed = 1.4  # 도보 속도 (m/s)
    INF = math.inf

    # stop_id별 stop_times를 그룹화하여 딕셔너리화
    # key: 정류장 ID, value: 해당 정류장에서의 시간표 데이터프레임
    stop_groups = {sid: grp for sid, grp in feed_data.stop_times.groupby('stop_id')}

    # trip_id별로 stop_sequence 기준으로 정렬된 시간표 생성
    # key: 열차 ID, value: 해당 열차의 정렬된 시간표 데이터프레임
    trip_groups = {tid: grp.sort_values('stop_sequence') for tid, grp in feed_data.stop_times.groupby('trip_id')}

    # 추가 최적화: 각 trip의 시간표 데이터를 NumPy 배열로 변환해 캐싱 (벡터화 및 캐싱)
    trip_cache = {}

    # 도보로 이동 가능한 인접 정류장을 계산 (320m 이내)
    spatial_index = geo_data.sindex
    radius = 320.0

    foot_paths = defaultdict(list)  # 정류장에서 도보로 갈 수 있는 튜플 목록
    # key: 정류장 ID, value: (이웃 정류장, 도보 소요시간) 튜플 목록
    for station_id, row in geo_data.iterrows():
        station_buffer = row.geometry.buffer(radius)  # 현재 정류장을 기준으로 반경 버퍼 생성

        # 버퍼와 교차하는 정류장들을 찾음
        for idx in spatial_index.query(station_buffer, predicate="intersects"):
            neighbor_id = geo_data.index[idx]  # 이웃 정류장 ID
            if neighbor_id != station_id:  # 자기 자신 제외
                dist = row.geometry.distance(geo_data.loc[neighbor_id].geometry)  # 두 정류장 사이의 거리 (미터)
                if dist <= radius:
                    # 도보 소요시간은 거리/보행속도
                    foot_paths[station_id].append((neighbor_id, dist / walking_speed if dist > 0 else 0))

    # 모든 정류장 리스트
    all_stops = feed_data.stops['stop_id'].unique()

    # 라운드마다 모든 정류장에 도착 가능한 시간을 저장할 딕셔너리 리스트
    # key: 정류장 ID, value: 정류장에 도착한 시간
    # max_transfers + 1 라운드를 위해 생성, 초기값은 INF (도착 불가능)
    arrivals = [dict.fromkeys(all_stops, INF) for _ in range(max_transfers + 1)]

    # 라운드에서 최적 경로 추적을 위한 부모 정보를 저장할 딕셔너리 리스트
    parents = [dict.fromkeys(all_stops, None) for _ in range(max_transfers + 1)]

    # 각 라운드별로 업데이트된 정류장들을 추적 (탐색된 정류장 집합)
    updated_stops = [set() for _ in range(max_transfers + 1)]

    # 출발 정류장에 대해 시작 시각을 설정
    arrivals[0][from_stop_id] = departure_secs
    updated_stops[0].add(from_stop_id)

    rounds_stats = []  # 각 라운드별 통계 기록

    # 시간제한 (예: 3시간 이내, 10800초)
    time_limit = 10800

    # 라운드 반복
    for round_idx in range(max_transfers + 1):
        round_start_time = time.time()  # 라운드 시작 시간 기록
        foot_updates = 0  # 도보 업데이트 횟수
        route_updates = 0  # 노선 업데이트 횟수

        # 도보 확장 - 현재 라운드에서 도보로 인접 정류장으로 이동 가능한 경우 업데이트
        newly_updated = set()
        for stop_id in updated_stops[round_idx]:
            base_time = arrivals[round_idx][stop_id]  # 해당 정류장에서의 도착 시간
            for nbr_id, foot_time in foot_paths.get(stop_id, []):
                new_arrival = base_time + foot_time  # 도보 이동 후 도착 시간
                if new_arrival < arrivals[round_idx][nbr_id]:
                    arrivals[round_idx][nbr_id] = new_arrival
                    # 부모 정보 (이전 정류장, 현재 라운드, 이동 모드, 출발 시간, 도착 시간, 도보 소요시간)
                    parents[round_idx][nbr_id] = (stop_id, round_idx, 'foot', base_time, new_arrival, foot_time)
                    newly_updated.add(nbr_id)
                    foot_updates += 1
        updated_stops[round_idx].update(newly_updated)  # 현재 라운드에 새로 업데이트된 정류장 추가

        # 노선 확장 - 현재 정류장에서 열차를 타고 이동 가능한 정류장을 업데이트
        if round_idx < max_transfers:
            newly_updated_next = set()

            for station_id in updated_stops[round_idx]:
                t_base = arrivals[round_idx][station_id]
                if t_base == INF or station_id not in stop_groups:
                    continue

                effective_time = t_base  # 열차 출발 시간을 고려 (현재 시각)
                candidates = stop_groups[station_id]  # 이 정류장에서 출발하는 모든 시간표 후보

                # 시간제한 필터: 현재 시각부터 time_limit 이내의 열차만 고려
                filtered = candidates[
                    (candidates['departure_time'] >= effective_time) &
                    (candidates['departure_time'] <= effective_time + time_limit)
                ]
                if filtered.empty:
                    continue

                # 이미 정렬되어 있으므로 이진 탐색으로 시작 인덱스 찾기
                sorted_candidates = filtered.sort_values('departure_time').reset_index(drop=True)
                departure_times = sorted_candidates['departure_time'].values
                start_index = np.searchsorted(departure_times, effective_time)
                valid_candidates = sorted_candidates.iloc[start_index:]
                # 같은 trip_id별 가장 빠른 departure_time 행 선택
                best_rows = valid_candidates.groupby('trip_id', as_index=False).first()

                for _, best_row in best_rows.iterrows():
                    trip_id = best_row['trip_id']
                    origin_dep = best_row['departure_time']
                    origin_seq = best_row['stop_sequence']

                    wait_time = origin_dep - t_base  # 현재 정류장에서 열차 출발까지 대기 시간
                    if wait_time < 0:
                        continue

                    # 캐싱: 해당 trip의 시간표 데이터를 NumPy 배열로 변환하여 재사용
                    if trip_id in trip_cache:
                        seq_arr, arr_arr, stopid_arr = trip_cache[trip_id]
                    else:
                        trip_df = trip_groups[trip_id]
                        seq_arr = trip_df['stop_sequence'].to_numpy()
                        arr_arr = trip_df['arrival_time'].to_numpy()
                        stopid_arr = trip_df['stop_id'].to_numpy()
                        trip_cache[trip_id] = (seq_arr, arr_arr, stopid_arr)

                    # 이진 탐색으로 origin_seq 이후의 인덱스 찾기
                    start_pos = np.searchsorted(seq_arr, origin_seq)
                    # 열차를 타고 이동할 수 있는 모든 정류장에 대해 업데이트
                    for i in range(start_pos, len(seq_arr)):
                        dest_id = stopid_arr[i]  # 목적 정류장 ID
                        candidate_arrival = arr_arr[i]  # 해당 정류장에서의 도착 시간
                        if candidate_arrival < arrivals[round_idx + 1][dest_id]:
                            arrivals[round_idx + 1][dest_id] = candidate_arrival
                            # 부모 정보 (이전 정류장, 현재 라운드, 'trip:열차ID', 해당 열차의 출발 시각, 도착 시각, 대기 시간)
                            parents[round_idx + 1][dest_id] = (station_id, round_idx, f"trip:{trip_id}", origin_dep, candidate_arrival, wait_time)
                            newly_updated_next.add(dest_id)
                            route_updates += 1
            updated_stops[round_idx + 1] = newly_updated_next

        round_elapsed = time.time() - round_start_time  # 라운드 소요 시간 계산
        reached_count = sum(1 for sid in all_stops if arrivals[round_idx][sid] < INF)  # 이 라운드까지 도달한 정류장 수
        rounds_stats.append({
            '라운드': round_idx,
            '탐색 정류장 수': reached_count,
            '도보 업데이트': foot_updates,
            '노선 업데이트': route_updates,
            '소요시간(초)': round_elapsed
        })
        print(f"Round {round_idx}: 정류장 {reached_count}개, 도보 {foot_updates}건, 노선 {route_updates}건, 소요: {round_elapsed:.2f}초")

        if round_idx < max_transfers and len(updated_stops[round_idx + 1]) == 0:
            print(f"Round {round_idx + 1}: 업데이트 없음 -> 종료")
            break

    # 최종 경로 복원 - 각 정류장에 대해 가장 빠른 도착 시간을 찾고, 역으로 추적하여 경로 재구성
    final_result = {}
    for station_id in all_stops:
        best_arrival_time = INF
        best_round = None
        for rr in range(max_transfers + 1):
            if arrivals[rr][station_id] < best_arrival_time:
                best_arrival_time = arrivals[rr][station_id]
                best_round = rr
        if best_arrival_time == INF:
            continue  # 도달하지 못한 정류장은 제외

        total_time = best_arrival_time - departure_secs  # 전체 소요 시간

        path_stops = []
        schedule_data = []
        current_stop, current_round = station_id, best_round
        visited_set = set()  # 사이클을 방지하기 위한 집합

        while True:
            key = (current_stop, current_round)
            if key in visited_set:
                break  # 이미 방문한 정류장-라운드 조합이면 사이클이 있으므로 종료
            visited_set.add(key)
            path_stops.append(current_stop)
            parent_data = parents[current_round][current_stop]
            if parent_data is None:
                base_time = arrivals[0].get(current_stop, departure_secs)
                schedule_data.append((current_stop, base_time, base_time, 0, None))
                break
            schedule_data.append((current_stop, arrivals[current_round][current_stop], parent_data[3], parent_data[5], parent_data[2]))
            current_stop, current_round = parent_data[0], parent_data[1]
        path_stops.reverse()
        schedule_data.reverse()
        final_result[station_id] = (total_time, path_stops, schedule_data)

    return final_result, arrivals, parents, rounds_stats, all_stops, INF