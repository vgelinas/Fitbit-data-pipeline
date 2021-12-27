import argparse

# check if non-negative integer, otherwise raise argparse exception
def check_nonnegative_int(value):
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue
