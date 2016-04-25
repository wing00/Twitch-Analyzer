import datetime
import time
import plotly
import db_connect
from sklearn.externals import joblib
from application import app


def set_plotly_creds():
    """ plot.ly credentials for streaming API

    :return: None
    """
    plotly.tools.set_credentials_file(username=app.config['PLOTLY_NAME'], api_key=app.config['PLOTLY_API'])
    plotly.tools.set_credentials_file(app.config['STREAM_KEY'])
    return app.config['STREAM_KEY']


def create_stream_model(stream_ids, online):
    """ creates plot and a list of stream objects with identifiers

    :param online: (boolean) flag for posting to plot.ly
    :param stream_ids: (list) list of stream id api keys
    :return: (list) list of dict of stream objects
    """

    joblib.load('./models/test.dill')
    twitch = db_connect.Twitch.run_live()

    # creating plot
    predicted = starscream.predict(datetime.datetime.now())

    data = [
        dict(
            x=datetime.datetime.now(),
            y=predicted,
            type='scatter',
            name='Machine',
            stream=dict(token=stream_ids[1])
        ),
        dict(
            x=datetime.datetime.now(),
            y=twitch[0]['viewers'],
            type='scatter',
            mode='markers',
            name=twitch[0]['name'],
            stream=dict(token=stream_ids[0])
        ),
        dict(
            x=datetime.datetime.now(),
            y=twitch[0]['viewers'],
            type='scatter',
            name='',
            stream=dict(token=stream_ids[2])
        ),

    ]

    stream = [
        dict(
            stream_obj=plotly.plotly.Stream(stream_ids[0]),
            name=twitch[0]['name'],
            stream_id=stream_ids[0]
           ),
        dict(
            stream_obj=plotly.plotly.Stream(stream_ids[1]),
            name=twitch[0]['name'],
            stream_id=stream_ids[1]
           )
        ]

    layout = dict(title='Streaming')
    fig = dict(data=data, layout=layout)

    if online:
        url = plotly.plotly.plot(fig, filename='live')
        with open('./templates/plots/live.html', mode='wb+') as f:
            f.write(plotly.tools.get_embed(url))

    return stream

def create_stream_plot(stream_ids, online):
    """ creates plot and a list of stream objects with identifiers

    :param online: (boolean) flag for posting to plot.ly
    :param stream_ids: (list) list of stream id api keys
    :return: (list) list of dict of stream objects
    """

    data = []
    stream = []
    twitch = db_connect.Twitch.run_live()

    # creating plot
    for index, stream_id in enumerate(stream_ids):
        trace = dict(
            x=datetime.datetime.now(),
            y=twitch[index]['viewers'],
            type='scatter',
            name=twitch[index]['name'],
            stream=dict(token=stream_id)
        )
        data.append(trace)
        stream.append(dict(stream_obj=plotly.plotly.Stream(stream_id),
                           name=twitch[index]['name'],
                           stream_id=stream_id
                           )
                      )

    layout = dict(title='Streaming')
    fig = dict(data=data, layout=layout)

    if online:
        url = plotly.plotly.plot(fig, filename='live')
        with open('./templates/plots/live.html', mode='wb+') as f:
            f.write(plotly.tools.get_embed(url))

    return stream


def stream_data(stream_ids, stream):
    """ Opens stream for data input and sends data

    :param stream_ids: (list) list of stream id api keys
    :param stream: (list) list of dict of stream objects
    :return: None
    """

    map(lambda x: stream[x]['stream_obj'].open(), range(len(stream_ids)))

    print('streams open')
    # feeding data to plot
    counter = 0

    while True:
        twitch = db_connect.Twitch.run_live()

        for index, stream_id in enumerate(stream_ids):
            x = datetime.datetime.now()
            y = [item['viewers'] for item in twitch if stream[index]['name'] == item['name']][0]
            stream[index]['stream_obj'].write(dict(x=x, y=y))

        time.sleep(0.5)
        counter += 1
        if counter % 10 == 0:
            print(counter)


if __name__ == '__main__':
    stream_ids = set_plotly_creds()
    stream = create_stream_plot(stream_ids, online=True)
    stream_data(stream_ids, stream)
