# services/raptor/router.py
import time
import math
import numpy as np
from collections import defaultdict

class Raptor:
    def __init__(self, feed_data, geo_data, walking_speed=1.4, time_limit=10800):
        """
        feed_data: GTFS feed data object
        geo_data: GeoDataFrame (AEQD 좌표계)
        walking_speed: 보행 속도 (m/s)
        time_limit: 열차 탐색 시간 제한 (초)
        """
        self.feed_data = feed_data
        self.geo_data = geo_data
        self.walking_speed = walking_speed
        self.time_limit = time_limit
        self.INF = math.inf

    def _build_stop_groups(self):
        # 정류장별 stop_times 그룹 생성
        return {sid: grp for sid, grp in self.feed_data.stop_times.groupby('stop_id')}

    def _build_trip_groups(self):
        # trip_id별 정렬된 시간표 그룹 생성
        return {tid: grp.sort_values('stop_sequence') for tid, grp in self.feed_data.stop_times.groupby('trip_id')}

    def raptor_search(self, from_stop_id, departure_secs, max_transfers):
        # RAPTOR 알고리즘 수행
        stop_groups = self._build_stop_groups()
        trip_groups = self._build_trip_groups()
        trip_cache = {}

        # 도보로 이동 가능한 인접 정류장 계산 (320m 이내)
        spatial_index = self.geo_data.sindex
        radius = 320.0
        foot_paths = defaultdict(list)  # key: 정류장, value: (이웃 정류장, 도보 소요시간) 튜플 목록
        for station_id, row in self.geo_data.iterrows():
            station_buffer = row.geometry.buffer(radius)  # 현재 정류장을 기준으로 반경 버퍼 생성
            # 버퍼와 교차하는 정류장들을 찾음
            for idx in spatial_index.query(station_buffer, predicate="intersects"):
                neighbor_id = self.geo_data.index[idx]
                if neighbor_id != station_id:  # 자기 자신 제외
                    dist = row.geometry.distance(self.geo_data.loc[neighbor_id].geometry)  # 두 정류장 사이의 거리 (미터)
                    if dist <= radius:
                        # 도보 소요시간은 거리/보행속도 (0 이상)
                        foot_paths[station_id].append((neighbor_id, dist / self.walking_speed if dist > 0 else 0))

        # 모든 정류장 리스트
        all_stops = self.feed_data.stops['stop_id'].unique()
        # 라운드별 도착 시간을 저장할 딕셔너리 리스트 (최대 max_transfers+1 라운드)
        arrivals = [dict.fromkeys(all_stops, self.INF) for _ in range(max_transfers + 1)]
        # 라운드별 부모 정보를 저장할 딕셔너리 리스트 (경로 추적을 위함)
        parents = [dict.fromkeys(all_stops, None) for _ in range(max_transfers + 1)]
        # 각 라운드별 업데이트된 정류장 추적 (탐색된 정류장 집합)
        updated_stops = [set() for _ in range(max_transfers + 1)]
        # 출발 정류장에 대해 시작 시각을 설정
        arrivals[0][from_stop_id] = departure_secs
        updated_stops[0].add(from_stop_id)

        rounds_stats = []  # 각 라운드별 통계 기록

        for round_idx in range(max_transfers + 1):
            round_start_time = time.time()  # 라운드 시작 시간 기록
            foot_updates = 0  # 도보 업데이트 횟수
            route_updates = 0  # 노선 업데이트 횟수

            # 도보 확장: 현재 라운드에서 도보로 인접 정류장 이동
            newly_updated = set()
            for stop_id in updated_stops[round_idx]:
                base_time = arrivals[round_idx][stop_id]  # 해당 정류장에서의 도착 시간
                for nbr_id, foot_time in foot_paths.get(stop_id, []):
                    new_arrival = base_time + foot_time  # 도보 이동 후 도착 시간 계산
                    if new_arrival < arrivals[round_idx][nbr_id]:
                        arrivals[round_idx][nbr_id] = new_arrival
                        # 부모 정보: (이전 정류장, 현재 라운드, 이동 모드, 출발 시간, 도착 시간, 도보 소요시간)
                        parents[round_idx][nbr_id] = (stop_id, round_idx, 'foot', base_time, new_arrival, foot_time)
                        newly_updated.add(nbr_id)
                        foot_updates += 1
            updated_stops[round_idx].update(newly_updated)

            # 노선 확장: 현재 정류장에서 열차를 타고 이동 가능한 정류장 업데이트
            if round_idx < max_transfers:
                newly_updated_next = set()
                for station_id in updated_stops[round_idx]:
                    t_base = arrivals[round_idx][station_id]
                    if t_base == self.INF or station_id not in stop_groups:
                        continue
                    effective_time = t_base  # 현재 시각으로 설정
                    candidates = stop_groups[station_id]  # 해당 정류장에서 출발하는 열차 후보
                    # 시간 제한 내 열차 후보 필터링
                    filtered = candidates[
                        (candidates['departure_time'] >= effective_time) &
                        (candidates['departure_time'] <= effective_time + self.time_limit)
                    ]
                    if filtered.empty:
                        continue
                    # departure_time 기준 정렬 후, 이진 탐색으로 시작 인덱스 찾기
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
                        wait_time = origin_dep - t_base  # 대기 시간 계산
                        if wait_time < 0:
                            continue

                        # 캐싱: 해당 trip의 시간표 데이터를 NumPy 배열로 변환해 재사용
                        if trip_id in trip_cache:
                            seq_arr, arr_arr, stopid_arr = trip_cache[trip_id]
                        else:
                            trip_df = trip_groups[trip_id]
                            seq_arr = trip_df['stop_sequence'].to_numpy()
                            arr_arr = trip_df['arrival_time'].to_numpy()
                            stopid_arr = trip_df['stop_id'].to_numpy()
                            trip_cache[trip_id] = (seq_arr, arr_arr, stopid_arr)

                        # origin_seq 이후 정류장들에 대해 업데이트
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

            round_elapsed = time.time() - round_start_time  # 라운드 소요 시간 계산
            reached_count = sum(1 for sid in all_stops if arrivals[round_idx][sid] < self.INF)  # 도달한 정류장 수
            rounds_stats.append({
                'round': round_idx,
                'reached_stops': reached_count,
                'foot_updates': foot_updates,
                'route_updates': route_updates,
                'elapsed_time': round_elapsed
            })
            print(f"Round {round_idx}: 정류장 {reached_count}개, 도보 {foot_updates}건, 노선 {route_updates}건, 소요: {round_elapsed:.2f}초")

            if round_idx < max_transfers and len(updated_stops[round_idx + 1]) == 0:
                print(f"Round {round_idx + 1}: 업데이트 없음 -> 종료")
                break

        # 최종 경로 복원: 각 정류장에 대해 최단 경로 추적
        final_result = {}
        for station_id in all_stops:
            best_arrival_time = self.INF
            best_round = None
            for rr in range(max_transfers + 1):
                if arrivals[rr][station_id] < best_arrival_time:
                    best_arrival_time = arrivals[rr][station_id]
                    best_round = rr
            if best_arrival_time == self.INF:
                continue

            total_time = best_arrival_time - departure_secs  # 전체 소요 시간 계산
            path_stops = []
            schedule_data = []
            current_stop, current_round = station_id, best_round
            visited_set = set()  # 사이클 방지를 위한 집합
            while True:
                key = (current_stop, current_round)
                if key in visited_set:
                    break  # 이미 방문한 경우 종료
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

        return final_result, arrivals, parents, rounds_stats, all_stops, self.INF