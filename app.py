from flask import Flask, request, jsonify, send_from_directory
import os
import folium

from subway_raptor import (
    raptor_search,
    time_to_seconds,
    secs_to_hhmm,
    load_gtfs,
    create_gdf,
    build_station_data
)

app = Flask(__name__)

# GTFS 및 GeoDataFrame 초기화
GTFS_PATH = 'kr_subway_gtfs.zip'
feed = load_gtfs(GTFS_PATH)
gdf = create_gdf(feed)
station_data = build_station_data(feed)

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# 역 목록 JSON 반환
@app.route('/stations', methods=['GET'])
def stations():
    print(station_data)
    return jsonify(station_data)

# 경로 검색
@app.route('/find_route', methods=['POST'])
def find_route():
    try:
        from_station = request.form.get('from_station')
        to_station = request.form.get('to_station')
        departure_time = request.form.get('departure_time')
        if not all([from_station, to_station, departure_time]):
            return jsonify({'error': '필수 파라미터가 누락되었습니다.'}), 400

        departure_secs = time_to_seconds(departure_time)
        from_stop_id = from_station  # 실제로 station_data의 stop_id 값
        to_stop_id = to_station

        final, arrivals, parents, rounds_stats, stops, INF = raptor_search(
            feed, gdf, from_stop_id, departure_secs, 3
        )
        if to_stop_id not in final:
            return jsonify({'error': '경로를 찾지 못했습니다.'}), 404

        tot_time, route, sched_info = final[to_stop_id]
        total_minutes = int(tot_time / 60)

        # station_info_map: build_station_data() 결과 매핑
        station_info_map = {s['stop_id']: s for s in station_data}
        route_info = []
        for i, stop_id in enumerate(route):
            s_info = station_info_map.get(stop_id, {})
            arr_time = sched_info[i][1]
            dep_time = sched_info[i][2]
            route_info.append({
                'station': s_info.get('stop_name', stop_id),
                'arrival': secs_to_hhmm(arr_time) if arr_time < INF else "",
                'departure': secs_to_hhmm(dep_time) if dep_time < INF else "",
                'operator': s_info.get('operator', 'Unknown'),
                'line': s_info.get('line', 'Unknown'),
                'line_info': s_info.get('line_info', '')
            })

        # 경로 맵 생성: Folium으로 생성하여 static/route_result.html에 저장
        m = folium.Map(location=[37.5665, 126.9780], zoom_start=11)
        route_coords = []
        for stop_id in route:
            stop_row = feed.stops[feed.stops['stop_id'] == stop_id].iloc[0]
            route_coords.append([stop_row.stop_lat, stop_row.stop_lon])
            folium.CircleMarker(
                location=[stop_row.stop_lat, stop_row.stop_lon],
                radius=5,
                color='blue',
                fill=True
            ).add_to(m)
        folium.PolyLine(route_coords, weight=2, color='blue', opacity=0.8).add_to(m)
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
        m.save(os.path.join(static_dir, 'route_result.html'))

        return jsonify({
            'total_time': total_minutes,
            'route_info': route_info
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)