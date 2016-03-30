import db_connect
import plotly
from time import sleep

def create_html_base(table_name):
    table_name + ['all_time', 'time_series']
    for table in table_name:
        f = open('templates/' + table + '.html', mode='w+')
        f.write('test')
        f.close()


def create_plot_table(table_name, cur, online=False):
    """ generating pie chart for table

    :param table_name: (str) name of table
    :param cur: (object) cursor object for sqldb
    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    param = dict(
        name=table_name,
        namebomb=table_name + 'bomb',
        nameid=table_name + 'id'
    )

    query = '''
        SELECT %(name)s.%(name)s, SUM(game_name.viewer_total) AS viewers
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
    else:
        query += '''
          GROUP BY %(name)s.%(name)s
          ORDER BY viewers DESC;
    ''' % param

    cur.execute(query)

    rows = cur.fetchall()
    label, values = zip(*rows)

    data = [dict(
            labels=label,
            values=values,
            type='pie',
            textinfo='none'
            )]

    layout = plotly.graph_objs.Layout(
        title=table_name.title() + ' Distribution of All Games by Viewers',
        showlegend=True
    )

    fig = dict(data=data, layout=layout)

    div = plotly.offline.plot(fig, output_type='div')
    if online:
        url = plotly.plotly.plot(fig, filename=table_name)
        print(plotly.tools.get_embed(url))
    return div



def create_plot_all_time(cur, online=False):
    """ getting viewer distribution data and plotting

    :param cur: (object) cursor object for sqldb
    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    cur.execute('''SELECT name, viewer_total FROM game_name''')
    rows = cur.fetchall()

    label, values = zip(*rows)
    data = [dict(
            labels=label,
            values=values,
            type='pie',
            textinfo='none'
            )]
    layout = plotly.graph_objs.Layout(
        title='Total Viewer Distribution: All-Time ',
        showlegend=False
    )

    fig = dict(data=data, layout=layout)

    div = plotly.offline.plot(fig, output_type='div')
    if online:
        url = plotly.plotly.plot(fig, filename='All Time')
        print(plotly.tools.get_embed(url))
    return div


def create_plot_time_series(cur, online=False):
    """ getting time series data and plotting

    :param cur: (object) cursor object for sqldb
    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """
    cur.execute('''
                SELECT name, viewers, stamp FROM snapshot
                ORDER BY name, stamp ASC
                ''')
    rows = cur.fetchall()

    data = []
    labels = []
    viewers = []
    timestamps = []

    for row in rows:
        if not labels:
            labels = row[0]

        if labels == row[0]:
            viewers.append(row[1])
            timestamps.append(row[2])
        else:
            trace = plotly.graph_objs.Scatter(
                x=timestamps,
                y=viewers,
                name=labels
            )

            data.append(trace)

            labels = row[0]
            viewers = [row[1]]
            timestamps = [row[2]]

    layout = plotly.graph_objs.Layout(
        title='Time Series of All Games',
        showlegend=False
    )
    fig = dict(data=data, layout=layout)

    div = plotly.offline.plot(fig, output_type='div')
    if online:
        url = plotly.plotly.plot(fig, filename='Time Series')
        print(plotly.tools.get_embed(url))
    return div


def main():
    conn = db_connect.connect()
    cur = conn.cursor()

    table_names = ['rating', 'franchise', 'publisher', 'platform', 'genre', 'theme']

    f = open('templates/plots.html', mode='w+')

    for name in table_names:
        div = create_plot_table(name, cur)
        f.write(div)
    div = create_plot_all_time(cur)
    f.write(div)
    conn.close()

    conn = db_connect.connect()
    cur = conn.cursor()
    div = create_plot_time_series(cur)
    f.write(div)
    f.close()
    conn.close()


    print("success")


main()
