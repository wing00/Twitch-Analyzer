import datetime
import time
import plotly

import db_connect
from application import app


def set_plotly_creds():
    """ plot.ly credentials for streaming API

    :return: None
    """

    plotly.tools.set_credentials_file(app.config['STREAM_KEY'])


def create_stream_plot(stream_ids, online):
    """ creates plot and a list of stream objects with identifiers

    :param online: (boolean) flag for posting to plot.ly
    :param stream_ids: (list) list of stream id api keys
    :return: (list) list of dict of stream objects
    """

    data = []
    stream = []
    twitch = db_connect.Twitch()

    # creating plot
    for index, stream_id in enumerate(stream_ids):
        trace = plotly.graph_objs.Scatter(
            x=[],
            y=[],
            name=twitch.fields[index]['name'],
            stream=plotly.graph_objs.Stream(token=stream_id)
        )
        data.append(trace)
        stream.append(dict(stream_obj=plotly.plotly.Stream(stream_id),
                           name=twitch.fields[index]['name'],
                           stream_id=stream_id
                           )
                      )
    layout = plotly.graph_objs.Layout(title='Streaming Time Series')
    fig = dict(data=data, layout=layout)
    if online:
        url = plotly.plotly.plot(fig, filename='live_stream')
        print(plotly.tools.get_embed(url))
    return stream


def stream_data(stream_ids, stream):
    """ Opens stream for data input and sends data

    :param stream_ids: (list) list of stream id api keys
    :param stream: (list) list of dict of stream objects
    :return: None
    """

    for index, stream_id in enumerate(stream_ids):
        stream[index]['stream_obj'].open()
    print('streams open')

    # feeding data to plot
    counter = 0
    while True:
        twitch = db_connect.Twitch()

        for index, stream_id in enumerate(stream_ids):

            y = twitch.fields[twitch.index_name(stream[index]['name'])]['viewers']
            x = datetime.datetime.now()
            stream[index]['stream_obj'].write(dict(x=x, y=y))

        time.sleep(0.5)

        counter += 1
        if counter % 10 == 0:
            print(counter)


def main():
    stream_ids = plotly.tools.get_credentials_file()['stream_ids']
    if not stream_ids:
        set_plotly_creds()
    stream = create_stream_plot(stream_ids, online=True)
    stream_data(stream_ids, stream)


if __name__ == '__main__':
    main()
