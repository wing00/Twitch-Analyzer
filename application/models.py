from application import app
from plotly_stream import set_plotly_creds, create_stream_model, stream_model_data


def create_stream(name):
    stream_ids = set_plotly_creds()
    div, stream = create_stream_model(stream_ids, name)

    return div, stream