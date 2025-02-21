from flask import Flask, request, jsonify, send_from_directory
import os

# subway_raptor 모듈에서 주요 함수 임포트
from subway_raptor import (
    raptor_search,     # RAPTOR 알고리즘을 실행하는 함수
    time_to_seconds,   # "HH:MM" 또는 "HH:MM:SS" 형식의 시간을 초 단위로 변환
    secs_to_hhmm,      # 초 단위의 시간을 "HH:MM" 형식으로 변환
    load_gtfs,         # GTFS 데이터 로드 및 전처리 함수
    create_gdf,        # 정류장 좌표를 이용해 GeoDataFrame 생성 및 투영 변환
    build_station_data # 여러 GTFS 테이블을 병합해 역 정보를 생성하는 함수
)

# 지도 그리기 함수 임포트
from map_drawer import draw_route_on_map  # 경로와 역 정보를 기반으로 지도에 노선을 그리는 함수

app = Flask(__name__)

# GTFS 데이터 로드 및 전처리
GTFS_PATH = 'kr_subway_gtfs.zip'
feed = load_gtfs(GTFS_PATH)         # GTFS 데이터 (정류장, 트립, 노선, 시간표 등)
gdf = create_gdf(feed)              # 정류장 좌표 기반 GeoDataFrame (거리 계산 용)
station_data = build_station_data(feed)  # 통합 역 정보 (stop_id, 역 이름, 노선 등)

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

# 경로 탐색 API: 클라이언트로부터 출발역, 도착역, 출발시간을 받아 경로를 탐색
@app.route('/find_route', methods=['POST'])
def find_route():
    try:
        # 요청 파라미터 추출
        from_station = request.form.get('from_station')
        to_station = request.form.get('to_station')
        departure_time = request.form.get('departure_time')
        if not all([from_station, to_station, departure_time]):
            return jsonify({'error': '필수 파라미터가 누락되었습니다.'}), 400

        # 출발 시간을 초 단위로 변환
        departure_secs = time_to_seconds(departure_time)

        # RAPTOR 알고리즘 실행
        # feed: GTFS 데이터, gdf: 정류장 좌표, from_station: 출발역, departure_secs: 출발 시간, max_transfers: 최대 환승 횟수
        final_result, _, _, rounds_stats, stops, INF = raptor_search(
            feed, gdf, from_station, departure_secs, max_transfers=3
        )
        if to_station not in final_result:
            return jsonify({'error': '경로를 찾지 못했습니다.'}), 404

        # 최종 경로 결과 복원 (총 소요시간, 정류장 순서, 스케줄 정보)
        tot_time, route, sched_info = final_result[to_station]
        total_minutes = int(tot_time / 60)

        # 역 정보 매핑: stop_id -> 역 정보
        station_info_map = {s['stop_id']: s for s in station_data}

        # 경로에 따른 역 정보를 구성
        route_info = []
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

        # 지도 그리기: draw_route_on_map() 함수는 feed, 경로, 그리고 각 역의 정보를 이용하여
        # 지도에 원형 마커와 노선선을 표시
        m = draw_route_on_map(feed, route, route_info)
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
        map_file = os.path.join(static_dir, 'route_result.html')
        m.save(map_file)

        # 결과를 JSON으로 반환 (소요시간, 경로 정보, 탐색 통계, 지도 파일 URL)
        return jsonify({
            'total_time': total_minutes,
            'route_info': route_info,
            'route': route,
            'rounds_stats': rounds_stats,
            'map_url': '/route_result.html'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Flask 서버 실행
    app.run(host='0.0.0.0', port=5001, debug=True)