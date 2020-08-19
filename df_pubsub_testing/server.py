from flask import Flask
from json import load

app = Flask(__name__)

BASE_URL = ""
PORT = 0
with open("config.json") as f:
    config_data = load(f)
    BASE_URL = config_data.get("BASE_URL")
    PORT = config_data.get("PORT")


@app.route(BASE_URL)
def hello_world():
    return 'Hello, World!'


if __name__ == '__main__':
    app.run(port=PORT)
