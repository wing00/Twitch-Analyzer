from application import app
from db import plots


def create_stream(name):
    return plots.run_stream(name)
