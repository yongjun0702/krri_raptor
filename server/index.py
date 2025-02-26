# server/index.py
from flask import Blueprint, current_app, send_from_directory

index_bp = Blueprint('index', __name__)

@index_bp.route('/')
def index():
    return send_from_directory(current_app.static_folder, 'index.html')