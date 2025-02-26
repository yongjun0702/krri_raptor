# server/__init__.py
from flask import Flask
from config import GTFS_DATA_PATH
from services.raptor import load_gtfs, create_gdf, build_station_data
from utils.logging import setup_logging
from .index import index_bp
from .route_api import api_bp

logger = setup_logging()

def create_app():
    app = Flask(__name__, static_folder='../static', static_url_path='/static')
    logger.info("GTFS 데이터 로드 시작...")
    gtfs_feed = load_gtfs(GTFS_DATA_PATH)
    stations_gdf = create_gdf(gtfs_feed)
    station_metadata = build_station_data(gtfs_feed)

    # 로드한 데이터를 앱 설정에 저장
    app.config['GTFS_FEED'] = gtfs_feed
    app.config['STATIONS_GDF'] = stations_gdf
    app.config['STATION_METADATA'] = station_metadata

    # Blueprint 등록
    app.register_blueprint(index_bp)
    app.register_blueprint(api_bp)
    logger.info("앱 초기화 완료.")
    return app