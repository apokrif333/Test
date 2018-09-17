import pandas as pd
import csv
import numpy as np
import time

from datetime import datetime
from termcolor import colored
from pprint import pprint as pp


def create_file():
    t1 = time.time()
    with open('Temp.txt', 'w') as f:
        for _ in range(4_000_000):
            f.write(str(10 * np.random.random()) + ',')
    t2 = time.time()
    print(f'Time for create file by  is {t2 - t1} seconds')


def little_file():
    np_array = np.random.rand(4, 5, 5)
    np.save('Temp.npy', np_array)


def filter_by_column(file: np.ndarray, column: int, value: float) -> np.array:
    pp(file)
    return file[file[:, :, column] > value][:, np.array([False, False, True, False, True])]

# create_file()
# with open('Temp.txt', 'r') as f:
#     f_data = f.read()
# list_ = f_data.split(',')
# list_.pop()
# np_array = np.array(list_, dtype=float).reshape(2000, 2000)
# pp(np_array)
# np.save('Temp.npy', np_array)


pp(filter_by_column(np.load('Temp.npy'), 0, 0.5))

