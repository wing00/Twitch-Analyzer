from argparse import ArgumentParser
from train import train_full_model, run_time_model
from predict import run_predict_time
import numpy


def parse_options():
    """ Flags to control which functions to run
    """
    parser = ArgumentParser()

    parser.add_argument('-f', '--full', default=False, action='store_true')
    parser.add_argument('-t', '--time',  type=int)
    parser.add_argument('-p', '--predict', default=False, action='store_true')
    return parser.parse_args()


if __name__ == '__main__':
    options = parse_options()

    if options.full:
        train_full_model()
    if options.time:
        run_time_model(options.time)
    if options.predict:
        run_predict_time()
