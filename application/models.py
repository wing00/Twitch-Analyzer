from application import app
from redis import ConnectionError
from worker import conn
from rq import Queue
from db import plots


def create_stream(name):
    div, stream = plots.run_stream(name)

    try:
        queue = Queue(connection=conn)
        queue.enqueue(plots.stream_model_data, stream)

    except ConnectionError:
        pass

    return div
