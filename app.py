from flask import Flask, request, jsonify, send_from_directory
import os

# subway_raptor 모듈에서 주요 함수 임포트
from subway_raptor import (
    raptor_search,     # RAPTOR 알고리즘 실행 함수
    time_to_seconds,   # 시간을 초로 변환
    secs_to_hhmm,      # 초를 "HH:MM" 형식으로 변환
    load_gtfs,         # GTFS 데이터 로드 및 전처리 함수
    create_gdf,        # GeoDataFrame 생성 함수
    build_station_data, filter_seoul_feed  # 역 정보 생성 및 서울 필터링 함수
)
# 지도 그리기 함수 임포트
from map_drawer import draw_route_on_map  # 지도에 경로 그리는 함수

app = Flask(__name__)

# GTFS 데이터 로드 및 전처리
GTFS_PATH = 'kr_subway_gtfs.zip'
feed = load_gtfs(GTFS_PATH)
USE_SEOUL = True  # True이면 서울 데이터만 사용
if USE_SEOUL:
    feed = filter_seoul_feed(feed)
gdf = create_gdf(feed)
station_data = build_station_data(feed)

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
    return jsonify(station_data)

# 경로 탐색 API
@app.route('/find_route', methods=['POST'])
def find_route():
    try:
        from_station = request.form.get('from_station')
        to_station = request.form.get('to_station')
        departure_time = request.form.get('departure_time')
        if not all([from_station, to_station, departure_time]):
            return jsonify({'error': '필수 파라미터가 누락되었습니다.'}), 400

        departure_secs = time_to_seconds(departure_time)
        final_result, _, _, rounds_stats, stops, INF = raptor_search(
            feed, gdf, from_station, departure_secs, max_transfers=3
        )
        if to_station not in final_result:
            return jsonify({'error': '경로를 찾지 못했습니다.'}), 404

        tot_time, route, sched_info = final_result[to_station]
        total_minutes = int(tot_time / 60)

        # 역 정보 매핑: stop_id -> 역 정보
        station_info_map = {s['stop_id']: s for s in station_data}
        route_info = []
        # 각 정류장에 대한 정보와 스케줄(시간) 정보를 포함하도록 구성
        for sid in route:
            s_info = station_info_map.get(sid, {
                'stop_name': sid,
                'operator': 'Unknown',
                'line': 'Unknown',
                'line_info': ''
            })
            route_info.append({
                'station': s_info['stop_name'],
                'stop_id': sid,
                'operator': s_info['operator'],
                'line': s_info['line'],
                'line_info': s_info['line_info']
            })

        m = draw_route_on_map(feed, route, route_info)
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
        map_file = os.path.join(static_dir, 'route_result.html')
        m.save(map_file)

        return jsonify({
            'total_time': total_minutes,
            'route_info': route_info,
            'route': route,
            'rounds_stats': rounds_stats,
            'schedule': sched_info,
            'map_url': '/route_result.html'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)