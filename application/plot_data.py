import db_connect
import plotly
from application import app
from multiprocessing import Pool


def create_plot_table(table_name, online=False):
    """ generating pie chart for table

    :param table_name: (str) name of table
    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    conn = db_connect.connect()
    cur = conn.cursor()

    param = dict(
        name=table_name,
        namebomb=table_name + 'bomb',
        nameid=table_name + 'id'
    )

    query = '''SELECT %(name)s.%(name)s, SUM(game_name.viewer_total) AS viewers
                  FROM %(namebomb)s
                  INNER JOIN game_name
                    ON game_name.giantbombid = %(namebomb)s.giantbombid
                  INNER JOIN %(name)s
                    ON %(namebomb)s.%(nameid)s = %(name)s.%(nameid)s''' % param

    if table_name == 'rating':
        query += '''
            WHERE rating.rating LIKE 'ESRB%'
            GROUP BY rating.rating
            ORDER BY rating.rating ASC;
        '''
    elif table_name =='platform':
        query = '''
            SELECT platformgroup.platformgroup, SUM(game_name.viewer_total) AS viewers
                FROM platformbomb
                INNER JOIN game_name
                  ON game_name.giantbombid = platformbomb.giantbombid
                INNER JOIN platform
                  ON platformbomb.platformid = platform.platformid
                INNER JOIN platformgroup
                  ON platformgroup.platformid = platform.platformid
                GROUP BY platformgroup.platformgroup
                ORDER BY viewers DESC;
        '''

    else:
        query += '''
          GROUP BY %(name)s.%(name)s
          ORDER BY viewers DESC;
    ''' % param

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    if table_name == 'genre':
        total = sum(value for name, value in rows)
        rows = [(name, value) for name, value in rows if value > 0.01 * total] + [('Other', sum(value for name, value in rows if value <= 0.01 * total))]

    label, values = zip(*sorted(rows, key=lambda x: -x[1]))

    data = [dict(
            y=values,
            x=label,
            marker=dict(color=['#1F77B4', '#FF7F0E', '#2CA02C', '#D62728', '#103D5D', '#9467BD', '#17BECF', '#E377C2', '#FFB574', '#57A9E2']),
            type='bar',
            )]

    layout = dict(
        title=table_name.title() + ' By Viewers',
        titlefont=dict(
            size=48,
        ),
        showlegend=False
    )

    fig = dict(data=data, layout=layout)

    plotly.plotly.image.save_as(fig, filename='static/img/plots/' + table_name, format='png')

    div = plotly.offline.plot(fig, output_type='div')
    with open('templates/plots/' + table_name + '.html', mode='w+') as f:
        f.write(div)

    if online:
        url = plotly.plotly.plot(fig, filename=table_name)
        print(plotly.tools.get_embed(url))


def create_plot_all_time(online=False):
    """ getting viewer distribution data and plotting

    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    conn = db_connect.connect()
    cur = conn.cursor()
    cur.execute('''SELECT name, viewer_total FROM game_name''')
    rows = cur.fetchall()

    total = sum(value for name, value in rows)
    new_rows = sorted([(name[:17], value) for name, value in rows if value > 0.005 * total], key=lambda x: x[1])
               #+ [('Other', sum(value for name, value in rows if value <= 0.01 * total))]

    label, values = zip(*new_rows)
    data = [dict(
            y=label,
            x=values,
            type='bar',
            orientation='h',
            )]

    layout = dict(
        title='Games By Viewers',
        titlefont=dict(
            size=48,
        ),

        margin=dict(
            l=100,
            pad=4
        ),

        yaxis=dict(
            tickangle=-45,
            tickfont=dict(
                size=8
            ),
        ),
        showlegend=False,
    )

    fig = dict(data=data, layout=layout)

    plotly.plotly.image.save_as(fig, filename='static/img/plots/alltime', format='png')
    div = plotly.offline.plot(fig, output_type='div')

    with open('templates/plots/alltime.html', mode='w+') as f:
        f.write(div)

    if online:
        url = plotly.plotly.plot(fig, filename='All Time')
        print(plotly.tools.get_embed(url))

    conn.close()


def create_plot_time_series(online=False):
    """ getting time series data and plotting

    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    conn = db_connect.connect()
    cur = conn.cursor()
    cur.execute('''
                SELECT name, viewers, stamp FROM snapshot
                WHERE stamp >= '2016-04-02'
                ORDER BY name, stamp ASC
                ''')

    rows = cur.fetchall()

    data = []
    labels = []
    viewers = []
    timestamps = []
    count = 0

    for row in rows:
        if not labels:
            labels = row[0]

        if labels == row[0]:
            viewers.append(row[1])
            timestamps.append(row[2])
        else:
            count += 1

            trace = plotly.graph_objs.Scatter(
                x=timestamps,
                y=viewers,
                name=labels,
            )

            data.append(trace)

            labels = row[0]
            viewers = [row[1]]
            timestamps = [row[2]]

    layout = dict(
        title='Games - Time Series',
        titlefont=dict(
            size=48,
        ),
        showlegend=False
        )
    fig = dict(data=data, layout=layout)

    div = plotly.offline.plot(fig, output_type='div')
    with open('templates/plots/timeseries.html', mode='w+') as f:
        f.write(div)

    plotly.plotly.image.save_as(fig, filename='static/img/plots/timeseries', format='png')
    if online:
        url = plotly.plotly.plot(fig, filename='Time Series')
        print(plotly.tools.get_embed(url))

    conn.close()


if __name__ == '__main__':

    # unused categories: 'publisher', 'franchise', 'theme'
    plotly.tools.set_credentials_file(username=app.config['PLOTLY_NAME'], api_key=app.config['PLOTLY_API'])

    table_names = ['rating', 'platform', 'genre', 'publisher', 'franchise', 'theme']
    pool = Pool()
    pool.map(create_plot_table, table_names)
    create_plot_all_time()
    # create_plot_time_series()

