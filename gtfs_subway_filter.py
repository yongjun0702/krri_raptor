import pandas as pd

stops_file = "ktdb_gtfs/stops.txt"
stop_times_file = "ktdb_gtfs/stop_times.txt"
trips_file = "ktdb_gtfs/trips.txt"
routes_file = "ktdb_gtfs/routes.txt"

# 1. stops.txt 파일 읽기 및 필터링
df_stops = pd.read_csv(stops_file, delimiter=",")
df_stops_filtered = df_stops[df_stops["stop_id"].str.startswith("RS_ACC1_S-1")]
print("필터링된 stops.txt:")
print(df_stops_filtered)

# 2. stop_times.txt 파일 읽기 및 필터링
df_stop_times = pd.read_csv(stop_times_file, delimiter=",")
# stops.txt에서 필터링된 정류장(stop_id) 목록 사용
filtered_stop_ids = df_stops_filtered["stop_id"].unique()
df_stop_times_filtered = df_stop_times[df_stop_times["stop_id"].isin(filtered_stop_ids)]
print("\n필터링된 stop_times.txt:")
print(df_stop_times_filtered)

# 3. trips.txt 파일 읽기 및 필터링
df_trips = pd.read_csv(trips_file, delimiter=",")
# stop_times.txt에서 사용된 trip_id 목록 추출
filtered_trip_ids = df_stop_times_filtered["trip_id"].unique()
df_trips_filtered = df_trips[df_trips["trip_id"].isin(filtered_trip_ids)]
print("\n필터링된 trips.txt:")
print(df_trips_filtered)

# 4. routes.txt 파일 읽기 및 필터링
df_routes = pd.read_csv(routes_file, delimiter=",")
# trips.txt에서 사용된 route_id 목록 추출
filtered_route_ids = df_trips_filtered["route_id"].unique()
df_routes_filtered = df_routes[df_routes["route_id"].isin(filtered_route_ids)]
print("\n필터링된 routes.txt:")
print(df_routes_filtered)

# 결과 파일로 저장
df_stops_filtered.to_csv("kr_subway_gtfs/stops.txt", index=False, encoding="utf-8-sig")
df_stop_times_filtered.to_csv("kr_subway_gtfs/stop_times.txt", index=False, encoding="utf-8-sig")
df_trips_filtered.to_csv("kr_subway_gtfs/trips.txt", index=False, encoding="utf-8-sig")
df_routes_filtered.to_csv("kr_subway_gtfs/routes.txt", index=False, encoding="utf-8-sig")

print("\n필터링된 파일들이 생성되었습니다.")