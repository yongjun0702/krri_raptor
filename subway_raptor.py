import partridge as ptg
import geopandas as gpd
import pyproj
from shapely.geometry import Point
import time
import pandas as pd
from collections import defaultdict
import datetime
import math


# 시간 변환 함수
def time_to_seconds(t):
    parts = t.split(':')
    if len(parts) == 2:
        h, m = map(int, parts)
        return h * 3600 + m * 60
    elif len(parts) == 3:
        h, m, s = map(int, parts)
        return h * 3600 + m * 60 + s
    raise ValueError("Time must be in HH:MM or HH:MM:SS format")


def secs_to_hhmm(secs):
    return (datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=secs)).strftime("%H:%M")


# GTFS 데이터 로드
def load_gtfs(gtfs_path='kr_subway_gtfs.zip'):
    _date, service_ids = ptg.read_busiest_date(gtfs_path)
    view = {'trips.txt': {'service_id': service_ids}}
    feed = ptg.load_feed(gtfs_path, view)
    for col in ['departure_time', 'arrival_time']:
        if feed.stop_times[col].dtype == object:
            feed.stop_times[col] = feed.stop_times[col].apply(time_to_seconds)
    return feed


# 정류장 GeoDataFrame 생성 (좌표 변환)
def create_gdf(feed):
    gdf = gpd.GeoDataFrame(
        {"stop_id": feed.stops.stop_id.tolist()},
        geometry=[Point(lon, lat) for lat, lon in zip(feed.stops.stop_lat, feed.stops.stop_lon)]
    )
    gdf = gdf.set_index("stop_id")
    gdf.crs = 'epsg:4326'
    centroid = gdf.iloc[0].geometry.centroid
    aeqd_crs = pyproj.CRS(proj='aeqd', ellps='WGS84', datum='WGS84',
                          lat_0=centroid.y, lon_0=centroid.x)
    gdf = gdf.to_crs(crs=aeqd_crs)
    return gdf


# 정류장 정보 구축 (한 번에 그룹화)
def build_station_data(feed):
    # stops: stop_id, stop_name
    df_stops = feed.stops[['stop_id', 'stop_name']]
    # trips: trip_id -> route_id
    df_trips = feed.trips[['trip_id', 'route_id']]
    # routes: route_id -> route_short_name, agency_id
    df_routes = feed.routes[['route_id', 'route_short_name', 'agency_id']]
    # stop_times: stop_id, trip_id
    df_stop_times = feed.stop_times[['stop_id', 'trip_id']]

    # 한 번에 병합: stop_times -> trips -> routes -> stops
    df = pd.merge(df_stop_times, df_trips, on='trip_id', how='left')
    df = pd.merge(df, df_routes, on='route_id', how='left')
    df = pd.merge(df, df_stops, on='stop_id', how='left')

    # stop_id별 그룹화하여 첫 번째 행(모든 행에서 stop_name은 동일하다고 가정)
    grouped = df.groupby('stop_id', as_index=False).first()

    station_data = []
    for _, row in grouped.iterrows():
        stop_id = row['stop_id']
        stop_name = row['stop_name'] if pd.notna(row['stop_name']) else 'Unknown'
        operator = row['agency_id'] if pd.notna(row['agency_id']) else 'Unknown'
        line = row['route_short_name'] if pd.notna(row['route_short_name']) else 'Unknown'
        line_info = f"{line}" if line != 'Unknown' else ''
        station_data.append({
            'stop_id': stop_id,
            'stop_name': stop_name,
            'operator': operator,
            'line': line,
            'line_info': line_info
        })
    return station_data


# RAPTOR 알고리즘
def raptor_search(feed, gdf, from_stop_id, departure_secs, max_transfers):
    walking_speed = 1.4  # m/s

    # 그룹화: 각 정류장별 스케줄, 각 열차별 스케줄 (정렬)
    stop_groups = {s: group for s, group in feed.stop_times.groupby('stop_id')}
    trip_groups = {t: grp.sort_values('stop_sequence') for t, grp in feed.stop_times.groupby('trip_id')}

    # 도보 인접 정류장 (320m 반경) 계산
    spatial_index = gdf.sindex
    radius = 320.0
    footpaths = defaultdict(list)
    for stop_id, row in gdf.iterrows():
        buff = row.geometry.buffer(radius)
        for idx in spatial_index.query(buff, predicate="intersects"):
            nbr = gdf.index[idx]
            if nbr != stop_id and row.geometry.distance(gdf.loc[nbr].geometry) <= radius:
                footpaths[str(stop_id)].append(str(nbr))

    stops = feed.stops['stop_id'].unique()
    INF = math.inf
    arrivals = [dict.fromkeys(stops, INF) for _ in range(max_transfers + 1)]
    parents = [dict.fromkeys(stops, None) for _ in range(max_transfers + 1)]
    updated = [set() for _ in range(max_transfers + 1)]
    arrivals[0][from_stop_id] = departure_secs
    updated[0].add(from_stop_id)
    rounds_stats = []

    for r in range(max_transfers + 1):
        t_round = time.time()
        foot_updates = 0
        route_updates = 0

        # 도보 확장: 동적 환승시간 = (거리 / 보행속도)
        new_updated = set()
        for stop in updated[r]:
            t_base = arrivals[r][stop]
            for nbr in footpaths.get(stop, []):
                distance = gdf.loc[stop].geometry.distance(gdf.loc[nbr].geometry)
                transfer_time = distance / walking_speed
                t_new = t_base + transfer_time
                if t_new < arrivals[r][nbr]:
                    arrivals[r][nbr] = t_new
                    parents[r][nbr] = (stop, r, 'foot', t_base, t_new, transfer_time)
                    new_updated.add(nbr)
                    foot_updates += 1
        updated[r].update(new_updated)

        # 노선(트립) 확장: r==0이면 effective_time = t_base, r>=1이면 effective_time = t_base + dynamic_transfer_time
        new_updated_next = set()
        if r < max_transfers:
            for stop in updated[r]:
                t_base = arrivals[r][stop]
                if t_base == INF or stop not in stop_groups:
                    continue
                if r == 0:
                    effective_time = t_base
                else:
                    pinfo = parents[r][stop]
                    if pinfo is None:
                        dynamic_transfer_time = 0
                    else:
                        if pinfo[2].startswith("trip:"):
                            prev_stop = pinfo[0]
                            dynamic_transfer_time = gdf.loc[prev_stop].geometry.distance(
                                gdf.loc[stop].geometry) / walking_speed
                        else:
                            dynamic_transfer_time = 0
                    effective_time = t_base + dynamic_transfer_time

                candidates = stop_groups[stop]
                valid = candidates[candidates['departure_time'] >= effective_time]
                if valid.empty:
                    continue
                best = valid.sort_values('departure_time').groupby('trip_id', as_index=False).first()
                for _, rowv in best.iterrows():
                    trip_id = rowv['trip_id']
                    origin_dep = rowv['departure_time']
                    origin_seq = rowv['stop_sequence']
                    wait_time = origin_dep - t_base
                    if wait_time < 0:
                        continue
                    trip_df = trip_groups[trip_id]
                    segment = trip_df[trip_df['stop_sequence'] >= origin_seq]
                    for _, trow in segment.iterrows():
                        dest = trow['stop_id']
                        t_arr = trow['arrival_time']
                        if t_arr < arrivals[r + 1][dest]:
                            arrivals[r + 1][dest] = t_arr
                            parents[r + 1][dest] = (stop, r, f"trip:{trip_id}", origin_dep, t_arr, wait_time)
                            new_updated_next.add(dest)
                            route_updates += 1
            updated[r + 1] = new_updated_next

        t_round_elapsed = time.time() - t_round
        reached = sum(1 for s in stops if arrivals[r][s] < INF)
        rounds_stats.append({
            '라운드': r,
            '탐색 정류장 수': reached,
            '도보 업데이트': foot_updates,
            '노선 업데이트': route_updates,
            '소요시간(초)': t_round_elapsed
        })
        print(f"Round {r}: 정류장 {reached}개, 도보 {foot_updates}건, 노선 {route_updates}건, 소요: {t_round_elapsed:.2f}초")
        if r < max_transfers and len(updated[r + 1]) == 0:
            print(f"Round {r + 1}: 업데이트 없음 -> 종료")
            break

    # 최종 결과 복원
    final = {}
    for stop in stops:
        best_t = INF
        best_r = None
        for rr in range(max_transfers + 1):
            if arrivals[rr][stop] < best_t:
                best_t = arrivals[rr][stop]
                best_r = rr
        if best_t == INF:
            continue
        tot_time = best_t - departure_secs
        path = []
        sched = []
        cur, cur_r = stop, best_r
        while True:
            path.append(cur)
            par = parents[cur_r][cur]
            if par is None:
                if cur in stop_groups:
                    cand = stop_groups[cur]
                    valid = cand[cand['departure_time'] >= departure_secs]
                    dep = valid['departure_time'].min() if not valid.empty else departure_secs
                else:
                    dep = departure_secs
                sched.append((cur, arrivals[0][cur], dep, 0, None))
                break
            sched.append((cur, arrivals[cur_r][cur], par[3], par[5], par[2]))
            cur, cur_r = par[0], par[1]
        path.reverse()
        sched.reverse()
        final[stop] = (tot_time, path, sched)
    return final, arrivals, parents, rounds_stats, stops, math.inf