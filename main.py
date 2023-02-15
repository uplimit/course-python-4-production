from typing import List, Tuple, Dict, Generator
import numpy as np
from pprint import pprint
from utils import Stats
from tqdm import tqdm
import os


class Koala:
    def __init__(self, file_path: str) -> None:
        self._fp = file_path
        self._col_names = []
        self._sep = ","
        self._stats = None

        self._set_col_names()

    @staticmethod
    def to_float(val):
        try:
            return float(val)
        except:
            return None

    def _set_col_names(self) -> None:
        with open(self._fp) as f:
            first_row = f.readline().strip('\n')

        col_names = first_row.split(self._sep)
        self._col_names = col_names

    def file_reader(self) -> Generator:
        """
        Input : None
        Output : Generator

        This method should return an iterable generator. Upon iteration the data should be of type Dict
        For example if the file format is as below:

        StockCode    , Description    , UnitPrice  , Quantity, TotalPrice , Country
        22180        , RETROSPOT LAMP , 19.96      , 4       , 79.84      , Russia
        23017        , APOTHECARY JAR , 24.96      , 1       , 24.96      , Germany

        The generator function should return the rows in the below format:
        {
            'StockCode': '22180',
            'Description': 'RETROSPOT LAMP',
            'UnitPrice': 19.96,
            'Quantity': 4,
            'TotalPrice': 79.84,
            'Country': 'Russia',
        }
        """
        for n_row, row in enumerate(open(self._fp, "r")):
            row_vals = row.strip('\n').split(self._sep)
            row_vals = {key:value for key,value in zip(self._col_names, row_vals)}
            row_vals['n_row'] = n_row
            yield row_vals

    def aggregate(self, column_name: str) -> float:
        """
        Input : List[str]
        Output : Dict

        This method should use the generator function (`file_reader`) created above and return aggregate
        of the column mentioned in the `column_name` variable

        For example if the `column_name` -> 'TotalPrice' and the file format is as below:

        StockCode    , Description    , UnitPrice  , Quantity, TotalPrice , Country
        22180        , RETROSPOT LAMP , 19.96      , 4       , 79.84      , Russia
        23017        , APOTHECARY JAR , 24.96      , 1       , 24.96      , Germany
        84732D       , IVORY CLOCK    , 0.39       , 2       , 0.78       ,India

        aggregate should be 105.58
        """

        file_gen = self.file_reader()

        # skip first row as it is the column name
        _ = next(file_gen)

        aggregate = 0

        for row in tqdm(file_gen):
            if self.to_float(row[column_name]):
                aggregate += self.to_float(row[column_name])

        return aggregate

    def describe(self, column_names: List[str]):
        file_gen = self.file_reader()

        # skip first row as it is the column name
        _ = next(file_gen)

        stats = {name: Stats() for name in column_names}

        for row in tqdm(file_gen):
            for column_name in column_names:
                stats[column_name].update_stats(val=row[column_name])

        self._stats = stats
        for column_name, value in self._stats.items():
            pprint(column_name)
            pprint(value.get_stats())


def revenue_per_region(ko: Koala) -> Dict:
    """
    Input : object of instance type Class Koala
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

    fp = ko.file_reader()

    # skip first row as it is the column name
    _ = next(fp)

    aggregate = dict()

    for row in tqdm(fp):
        if row['Country'] not in aggregate:
            aggregate[row['Country']] = 0
        aggregate[row['Country']] += ko.to_float(row['TotalPrice'])

    return aggregate


def get_sales_information(file_path: str) -> Dict:
    # Initialize
    dp = Koala(file_path=file_path)

    # print stats
    dp.describe(column_names=['UnitPrice', 'TotalPrice'])

    # return total revenue and revenue per region
    return {
        'total_revenue': dp.aggregate(column_name='TotalPrice'),
        'revenue_per_region': revenue_per_region(dp)
    }


def main():
    folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    file_paths = [os.path.join(folder_path, file_name) for file_name in ['2015.csv', '2016.csv', '2017.csv',
                                                                         '2018.csv', '2019.csv', '2020.csv',
                                                                         '2021.csv']]
    revenue_data = [get_sales_information(file_path) for file_path in file_paths]
    pprint(revenue_data)


if __name__ == '__main__':
    main()
