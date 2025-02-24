from flask import Flask, request, jsonify, send_from_directory
import os

# subway_raptor 모듈에서 주요 함수 임포트
from subway_raptor import (
    raptor_search,          # RAPTOR 알고리즘 실행 함수
    time_to_seconds,        # "HH:MM" 또는 "HH:MM:SS" 시간을 초 단위로 변환
    secs_to_hhmm,           # 초 단위를 "HH:MM" 형식으로 변환
    load_gtfs,              # GTFS 데이터 로드 및 전처리 함수
    create_gdf,             # 정류장 좌표를 기반으로 GeoDataFrame 생성 및 투영 변환 함수
    build_station_data      # 여러 GTFS 테이블을 병합해 역 정보를 생성하는 함수
)

# 지도 그리기 함수 임포트
from map_drawer import draw_route_on_map  # 경로와 역 정보를 기반으로 지도에 노선을 그리는 함수

app = Flask(__name__)

# GTFS 데이터 로드 및 전처리
GTFS_DATA_PATH = 'kr_subway_gtfs.zip'
gtfs_feed = load_gtfs(GTFS_DATA_PATH)         # GTFS 데이터 (정류장, 트립, 노선, 시간표 등)
stations_gdf = create_gdf(gtfs_feed)          # 정류장 좌표 기반 GeoDataFrame (거리 계산용)
station_metadata = build_station_data(gtfs_feed)  # 통합 역 정보 (stop_id, 역 이름, 노선 등)

# 메인 페이지 제공
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

# 정적 파일 제공
@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# 역 정보 API
@app.route('/stations', methods=['GET'])
def stations():
    return jsonify(station_metadata)

# 경로 탐색 API: 클라이언트로부터 출발역, 도착역, 출발시간을 받아 경로 탐색
@app.route('/find_route', methods=['POST'])
def find_route():
    try:
        # 요청 파라미터 추출
        origin_station = request.form.get('from_station')
        destination_station = request.form.get('to_station')
        departure_time_str = request.form.get('departure_time')
        if not all([origin_station, destination_station, departure_time_str]):
            return jsonify({'error': '필수 파라미터가 누락되었습니다.'}), 400

        # 출발 시간을 초 단위로 변환
        departure_time_seconds = time_to_seconds(departure_time_str)

        # RAPTOR 알고리즘 실행
        # gtfs_feed: GTFS 데이터, stations_gdf: 정류장 좌표, origin_station: 출발역, departure_time_seconds: 출발 시간, max_transfers: 최대 환승 횟수
        route_search_results, _, _, rounds_statistics, stops, INF = raptor_search(
            gtfs_feed, stations_gdf, origin_station, departure_time_seconds, max_transfers=3
        )
        if destination_station not in route_search_results:
            return jsonify({'error': '경로를 찾지 못했습니다.'}), 404

        # 최종 경로 결과 복원 (총 소요시간, 정류장 순서, 스케줄 정보)
        total_time_seconds, station_route, schedule_info = route_search_results[destination_station]
        total_time_minutes = int(total_time_seconds / 60)

        # 역 정보 매핑: stop_id -> 역 메타정보
        station_info_mapping = {station['stop_id']: station for station in station_metadata}

        # route_details 구성:
        # schedule_info[i] = (stop_id, arrival_seconds, departure_seconds, wait_time, mode)
        # - 첫 정류장은 출발역, 마지막 정류장은 도착역, 중간 정류장은 도착/출발 모두 기록
        route_details = []
        for idx, stop_id in enumerate(station_route):
            station_info = station_info_mapping.get(stop_id, {})
            # schedule_info[idx] = (stop_id, arrival_seconds, departure_seconds, wait_time, mode)
            _, arrival_seconds, departure_seconds, _, _ = schedule_info[idx]

            arrival_time_str = ""
            departure_time_str = ""

            # 첫 정류장: 도착 시간 없음, "출발"은 다음 역의 실제 열차 출발 시간 사용
            if idx == 0:
                if len(station_route) == 1:
                    # 이동 없는 경우: 출발/도착 모두 지정된 시간 사용
                    departure_time_str = secs_to_hhmm(departure_seconds)
                else:
                    # 다음 역의 출발 시간 추출
                    _, _, next_departure_seconds, _, _ = schedule_info[idx+1]
                    departure_time_str = secs_to_hhmm(next_departure_seconds)

            # 마지막 정류장: 도착 시간만 표시
            elif idx == len(station_route) - 1:
                arrival_time_str = secs_to_hhmm(arrival_seconds)

            # 중간 정류장: 도착/출발 모두 표시
            else:
                arrival_time_str = secs_to_hhmm(arrival_seconds)
                if idx + 1 < len(station_route):
                    _, _, next_departure_seconds, _, _ = schedule_info[idx+1]
                    departure_time_str = secs_to_hhmm(next_departure_seconds)

            route_details.append({
                'station': station_info.get('stop_name', stop_id),
                'arrival': arrival_time_str,
                'departure': departure_time_str,
                'operator': station_info.get('operator', 'Unknown'),
                'line': station_info.get('line', 'Unknown'),
                'line_info': station_info.get('line_info', '')
            })

        # 지도 그리기: draw_route_on_map() 함수는 gtfs_feed, station_route, 그리고 각 역의 정보를 이용하여
        # 지도에 원형 마커와 노선선을 표시
        map_object = draw_route_on_map(gtfs_feed, station_route, route_details)
        static_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
        map_output_file = os.path.join(static_directory, 'route_result.html')
        map_object.save(map_output_file)

        # 결과를 JSON으로 반환 (소요시간, 경로 정보, 탐색 통계, 지도 파일 URL)
        return jsonify({
            'total_time': total_time_minutes,
            'route_info': route_details,
            'route': station_route,
            'rounds_stats': rounds_statistics,
            'map_url': '/route_result.html'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Flask 서버 실행
    app.run(host='0.0.0.0', port=5001, debug=True)