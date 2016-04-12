import db_connect
from argparse import ArgumentParser


def parse_options():
    """ Flags to control which functions to run
    """
    parser = ArgumentParser()

    parser.add_argument('-t', '--stream', default=False, action='store_true')
    parser.add_argument('-f', '--featured', default=False, action='store_true')
    parser.add_argument('-g', '--games', default=False, action='store_true')

    return parser.parse_args()


if __name__ == '__main__':
    options = parse_options()

    if options.games:
        db_connect.Twitch.run_fields()
    if options.stream:
        db_connect.Twitch.run_streams()
    if options.featured:
        db_connect.Twitch.run_featured()
