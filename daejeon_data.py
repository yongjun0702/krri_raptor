import pandas as pd

# 파일 읽기
df = pd.read_csv('minchan/ROUTE_20221114.csv', encoding='euc-kr')
stops = pd.read_csv('GTFS_DataSet/stops.txt')
routes = pd.read_csv('GTFS_DataSet/routes.txt')
trips = pd.read_csv('GTFS_DataSet/trips.txt')
stop_times = pd.read_csv('GTFS_DataSet/stop_times.txt')

# routes 데이터에서 'route_short_name' 컬럼에 해당 번호가 포함된 행만 필터링
filtered_routes = routes[
    routes['route_id'].astype(str).str.contains("DJB", case=False, na=False) |
    routes['route_short_name'].astype(str).str.contains("대전1호선", case=False, na=False)
]

# 필터링된 데이터를 하나의 CSV 파일로 저장 (한글 처리를 위해 utf-8-sig 사용)
filtered_routes.to_csv("dj_routes.csv", index=False, encoding='utf-8-sig')
print("모든 필터링된 데이터가 dj_routes.csv 에 저장되었습니다.")
  
# dj_routes 파일 읽기 (이전에 저장한 필터링된 routes 데이터)
dj_routes = pd.read_csv("dj_routes.csv", encoding='utf-8-sig')

# dj_routes에 포함된 고유의 route_id 목록 추출
filtered_route_ids = dj_routes['route_id']

# trips 데이터에서 route_id가 filtered_route_ids에 포함된 행 필터링
filtered_trips = trips[trips['route_id'].isin(filtered_route_ids)]

# 필터링된 데이터를 새로운 CSV 파일로 저장 (한글 처리를 위해 utf-8-sig 사용)
filtered_trips.to_csv("dj_trips.csv", index=False, encoding='utf-8-sig')
print("모든 필터링된 데이터가 dj_trips.csv 에 저장되었습니다.")

dj_trips = pd.read_csv("dj_trips.csv", encoding='utf-8-sig')

filtered_trip_ids = dj_trips['trip_id']

filtered_stop_times = stop_times[stop_times['trip_id'].isin(filtered_trip_ids)]

filtered_stop_times.to_csv("dj_stop_times.csv", index=False, encoding='utf-8-sig')
print("모든 필터링된 데이터가 dj_stop_times.csv 에 저장되었습니다.")

dj_stop_times = pd.read_csv("dj_stop_times.csv", encoding='utf-8-sig')

filtered_stop_ids = dj_stop_times['stop_id']

filtered_stops = stops[stops['stop_id'].isin(filtered_stop_ids)]

filtered_stops.to_csv("dj_stops.csv", index=False, encoding='utf-8-sig')
print("모든 필터링된 데이터가 dj_stops.csv 에 저장되었습니다.")