import time
from typing import List, Tuple, Dict, Generator
from tqdm import tqdm
import os
import multiprocessing
from w1.data_processor import DataProcessor
import constants
from global_utils import get_file_name, make_dir, plot_sales_data
import json
import argparse
from datetime import datetime
from pprint import pprint

CURRENT_FOLDER_NAME = os.path.dirname(os.path.abspath(__file__))


class DP(DataProcessor):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)

    def get_file_path(self) -> str:
        return self._fp

    def get_file_name(self) -> str:
        return self._file_name

    def get_n_rows(self) -> int:
        return self._n_rows


def revenue_per_region(dp: DP) -> Dict:
    """
    Input : object of instance type Class DP
    Output : Dict

    The method should find the aggregate revenue per region

    For example if the file format is as below:

    StockCode    , Description    , UnitPrice  , Quantity, TotalPrice , Country
    22180        , RETROSPOT LAMP , 19.96      , 4       , 79.84      , Russia
    23017        , APOTHECARY JAR , 24.96      , 1       , 24.96      , Germany
    84732D       , IVORY CLOCK    , 0.39       , 2       , 0.78       ,India
    ...
    ...
    ...

    expected output format is:
    {
        'China': 1.66,
        'France': 17.14,
        'Germany': 53.699999999999996,
        'India': 55.78,
        'Italy': 90.45,
        'Japan': 76.10000000000001,
        'Russia': 87.31,
        'United Kingdom': 29.05,
        'United States': 121.499
    }
    """

    data_reader = dp.data_reader
    data_reader_gen = (row for row in data_reader)

    # skip first row as it is the column name
    _ = next(data_reader_gen)

    aggregate = dict()

    for row in tqdm(data_reader_gen):
        if row[constants.OutDataColNames.COUNTRY] not in aggregate:
            aggregate[row[constants.OutDataColNames.COUNTRY]] = 0
        aggregate[row[constants.OutDataColNames.COUNTRY]] += dp.to_float(row[constants.OutDataColNames.TOTAL_PRICE])

    return aggregate


def get_sales_information(file_path: str) -> Dict:
    # Initialize
    dp = DP(file_path=file_path)

    # print stats
    dp.describe(column_names=[constants.OutDataColNames.UNIT_PRICE, constants.OutDataColNames.TOTAL_PRICE])

    # return total revenue and revenue per region
    return {
        'total_revenue': dp.aggregate(column_name=constants.OutDataColNames.TOTAL_PRICE),
        'revenue_per_region': revenue_per_region(dp),
        'file_name': get_file_name(file_path)
    }


# batches the files based on the number of processes
def batch_files(file_paths, n_processes):
    if n_processes > len(file_paths):
        return []

    n_per_batch = len(file_paths) // n_processes

    first_set_len = n_processes * n_per_batch
    first_set = file_paths[0:first_set_len]
    second_set = file_paths[first_set_len:]

    batches = [set(file_paths[i:i + n_per_batch]) for i in range(0, len(first_set), n_per_batch)]
    for ind, each_file in enumerate(second_set):
        batches[ind].add(each_file)

    return batches


# Fetch the revenue data from a file
def run(file_names, n_process):
    st = time.time()

    print("Thread : {}".format(n_process))
    folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    file_paths = [os.path.join(folder_path, file_name) for file_name in file_names]
    revenue_data = [get_sales_information(file_path) for file_path in file_paths]

    en = time.time()

    print(f"Batch for thread-{n_process} time taken {en - st}")
    return revenue_data


def flatten(l):
    return [item for sublist in l for item in sublist]


def main():
    """
    Use the `batch_files` method to create batches of files that needs to be run in each process
    Use the `run` method to fetch revenue data for a given batch of files

    Use multiprocessing module to process batches of data in parallel
    Check `multiprocessing.Pool` and `pool.starmap` methods to help you wit the task

    At the end check the overall time taken in this code vs the time taken in W1 code
    """
    st = time.time()
    n_processes = 3

    parser = argparse.ArgumentParser(description="Choose from one of these : [tst|sml|bg]")
    parser.add_argument('--type',
                        default='tst',
                        choices=['tst', 'sml', 'bg'],
                        help='Type of data to generate')
    args = parser.parse_args()

    data_folder_path = os.path.join(CURRENT_FOLDER_NAME, '..', constants.DATA_FOLDER_NAME, args.type)
    files = [str(file) for file in os.listdir(data_folder_path) if str(file).endswith('csv')]

    output_save_folder = os.path.join(CURRENT_FOLDER_NAME, '..', 'output', args.type,
                                      datetime.now().strftime("%B %d %Y %H-%M-%S"))
    make_dir(output_save_folder)
    file_paths = [os.path.join(data_folder_path, file_name) for file_name in files]

    batches = batch_files(file_paths=file_paths, n_processes=n_processes)

    with multiprocessing.Pool(processes=n_processes) as pool:
        # Apply the worker function to each argument in the list in parallel
        revenue_data = pool.starmap(run, [(batch, n_thread) for n_thread, batch in enumerate(batches)])
        revenue_data = flatten(l=revenue_data)
        # Close the pool and wait for all the tasks to complete
        pool.close()
        pool.join()

    en = time.time()
    print("Overall time taken : {}".format(en-st))

    for yearly_data in revenue_data:
        with open(os.path.join(output_save_folder, f'{yearly_data["file_name"]}.json'), 'w') as f:
            f.write(json.dumps(yearly_data))

        plot_sales_data(yearly_revenue=yearly_data['revenue_per_region'], year=yearly_data["file_name"],
                        plot_save_path=os.path.join(output_save_folder, f'{yearly_data["file_name"]}.png'))

    return revenue_data


if __name__ == '__main__':
    res = main()
    pprint(res)
