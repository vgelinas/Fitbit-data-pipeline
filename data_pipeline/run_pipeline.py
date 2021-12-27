from parser_utils import check_nonnegative_int
from pipeline import Pipeline
import argparse


if __name__ == "__main__":

    # collect user arguments when calling from command line to launch pipeline
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s",
        "--seconds_between_calls",
        type=check_nonnegative_int,
        help="number of seconds between fitbit api calls")

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="increase output verbosity")

    args = parser.parse_args()

    # turn args attributes into a dict, removing the None values
    # this way default arguments are used for Pipeline when no arg is supplied
    args = {k: v for k, v in vars(args).items() if v is not None}

    # launch pipeline
    Pipeline(**args).run()
