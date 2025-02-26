from server import create_app
from config import FLASK_HOST, FLASK_PORT, DEBUG

app = create_app()

if __name__ == '__main__':
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=DEBUG)