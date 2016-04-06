import db_connect
from argparse import ArgumentParser


def games():
    data = db_connect.Twitch.run_fields()

    for index, row in enumerate(data):
        db_connect.update_table(row)
        print(index)


def stream():
    stream_row, team_row = db_connect.Twitch.run_streams()

    for index, row in enumerate(stream_row):
        db_connect.update_stream_table(row)
        print index

    for index, row in enumerate(team_row):
        db_connect.update_team_table(row)
        print index


def featured():
    stream_row, team_row = db_connect.Twitch.run_featured()

    for index, row in enumerate(stream_row):
        db_connect.update_stream_table(row)
        print index

    print 'updating tables'
    for index, row in enumerate(team_row):
        db_connect.update_team_table(row)
        print index


def parse_options():
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






