# server/route_api.py
from flask import Blueprint, request, jsonify, current_app
import os
from services.raptor import raptor_search, time_to_seconds
from server.response_formatter import format_route_response
from config import MAX_TRANSFERS
from utils.logging import setup_logging

logger = setup_logging()
api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/find_route', methods=['POST'])
def find_route():
    try:
        # 요청 파라미터 추출
        origin_station = request.form.get('from_station')
        destination_station = request.form.get('to_station')
        departure_time_str = request.form.get('departure_time')
        if not all([origin_station, destination_station, departure_time_str]):
            return jsonify({'error': '필수 파라미터 누락'}), 400

        # 출발 시간을 초 단위로 변환
        departure_time_secs = time_to_seconds(departure_time_str)
        gtfs_feed = current_app.config.get('GTFS_FEED')
        stations_gdf = current_app.config.get('STATIONS_GDF')
        station_metadata = current_app.config.get('STATION_METADATA')

        # RAPTOR 알고리즘 실행
        result, arrivals, parents, rounds_stats, all_stops, INF = raptor_search(
            gtfs_feed, stations_gdf, origin_station, departure_time_secs, MAX_TRANSFERS
        )
        if destination_station not in result:
            return jsonify({'error': '경로를 찾지 못했습니다.'}), 404

        # Flask 앱의 정적 폴더를 사용
        static_dir = current_app.static_folder
        total_time, station_route, route_details, map_file = format_route_response(
            result[destination_station], station_metadata, gtfs_feed, static_dir
        )

        response_data = {
            'total_time': total_time,
            'route_info': route_details,
            'route': station_route,
            'rounds_stats': rounds_stats,
            'map_url': '/static/route_result.html'
        }
        return jsonify(response_data)
    except Exception as e:
        logger.exception("find_route 실행 중 오류")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/stations', methods=['GET'])
def get_stations():
    station_metadata = current_app.config.get('STATION_METADATA')
    return jsonify(station_metadata)