import os
import math


def make_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def human_readable(number):
    units = ['', ' Thousand', ' Million', ' Billion', ' Trillion']

    n = float(number)
    millidx = max(0 , min(len(units)-1, int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.0f}{}'.format(n / 10**(3 * millidx), units[millidx])
