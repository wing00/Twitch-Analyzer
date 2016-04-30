from application import app
# from redis import ConnectionError
# from worker import conn
# from rq import Queue
from db import plots
import json


def create_stream(name):
    div, stream = plots.run_stream(name)


    return div


def check_list(name):
    with open('application/static/games.json', mode='r') as f:
        data = json.loads(f.read())
    for item in data:
        if name == item['game']:
            return True
    return False
