import db_connect
from argparse import ArgumentParser


def games():
    """ Gets the top games and updates database

    """
    data = db_connect.Twitch.run_fields()

    for index, row in enumerate(data):
        db_connect.update_table(row)
        print index


def stream():
    """ Gets the all streams and updates database

    """
    stream_row, team_row = db_connect.Twitch.run_streams()

    for index, row in enumerate(stream_row):
        db_connect.update_stream_table(row)
        print index

    print 'updating tables'

    for index, row in enumerate(team_row):
        db_connect.update_team_table(row)
        print index


def featured():
    """Gets the featured streams and updates database

    """
    stream_row, team_row = db_connect.Twitch.run_featured()

    for index, row in enumerate(stream_row):
        db_connect.update_stream_table(row)
        print index

    print 'updating tables'

    for index, row in enumerate(team_row):
        db_connect.update_team_table(row)
        print index


def parse_options():
    """ Flags to control which functions to run
    """
    parser = ArgumentParser()

    parser.add_argument('-s', '--stream', default=False, action='store_true')
    parser.add_argument('-f', '--featured', default=False, action='store_true')
    parser.add_argument('-g', '--games', default=False, action='store_true')

    return parser.parse_args()


if __name__ == '__main__':
    options = parse_options()

    if options.games:
        games()
    if options.stream:
        stream()
    if options.featured:
        featured()
