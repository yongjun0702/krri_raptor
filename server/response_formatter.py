# server/response_formatter.py
import os
from services.raptor import secs_to_hhmm
from server.map_line_info import draw_route_on_map  # 지도 생성 함수

def format_route_response(route_result, station_metadata, gtfs_feed, static_directory,
                          output_filename="route_result.html"):
    """
    경로 탐색 결과를 가공하고 지도를 생성합

    Parameters:
        route_result: (total_time_seconds, station_route, schedule_info)
        station_metadata: 전체 역 정보 리스트
        gtfs_feed: GTFS 데이터 (정류장, 트립, 노선, 시간표 등)
        static_directory: 생성된 지도 파일을 저장할 정적 파일 디렉토리
        output_filename: 생성할 지도 파일명 (기본: "route_result.html")

    Returns:
        total_time_minutes: 총 소요 시간 (분 단위)
        station_route: 경로상의 정류장 ID 리스트
        route_details: 정류장별 상세 정보 리스트
        map_output_file: 생성된 지도 파일의 전체 경로
    """
    total_time_seconds, station_route, schedule_info = route_result
    total_time_minutes = int(total_time_seconds / 60)

    # 역 정보 매핑: stop_id -> 역 메타정보
    station_info_mapping = {station['stop_id']: station for station in station_metadata}

    route_details = []
    for idx, stop_id in enumerate(station_route):
        station_info = station_info_mapping.get(stop_id, {})
        # schedule_info[idx] = (stop_id, arrival_seconds, departure_seconds, wait_time, mode)
        _, arrival_seconds, departure_seconds, _, _ = schedule_info[idx]

        arrival_time_str = ""
        departure_time_str = ""

        if idx == 0:
            if len(station_route) == 1:
                departure_time_str = secs_to_hhmm(departure_seconds)
            else:
                _, _, next_departure_seconds, _, _ = schedule_info[idx + 1]
                departure_time_str = secs_to_hhmm(next_departure_seconds)
        elif idx == len(station_route) - 1:
            arrival_time_str = secs_to_hhmm(arrival_seconds)
        else:
            arrival_time_str = secs_to_hhmm(arrival_seconds)
            if idx + 1 < len(station_route):
                _, _, next_departure_seconds, _, _ = schedule_info[idx + 1]
                departure_time_str = secs_to_hhmm(next_departure_seconds)

        route_details.append({
            'station': station_info.get('stop_name', stop_id),
            'arrival': arrival_time_str,
            'departure': departure_time_str,
            'operator': station_info.get('operator', 'Unknown'),
            'line': station_info.get('line', 'Unknown'),
            'line_info': station_info.get('line_info', '')
        })

    # 지도 생성
    map_object = draw_route_on_map(gtfs_feed, station_route, route_details)
    map_output_file = os.path.join(static_directory, output_filename)
    map_object.save(map_output_file)

    return total_time_minutes, station_route, route_details, map_output_file
