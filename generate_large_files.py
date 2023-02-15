import json
import random
from tqdm import tqdm

from random import randrange
from datetime import timedelta, datetime


import uuid


def is_typecast_float(val):
    try:
        return float(val)
    except:
        return None


def items_details():
    with open('/Users/zignite/Projects/CoRise/w1/data/items.json') as f:
        items = json.loads(f.read())

    modified_items = []
    for stock_no, desc in items.items():
        desc['stock_no'] = stock_no
        if all([(isinstance(desc['description'], str) and len(desc['description']) > 0),
                (is_typecast_float(desc['unit_price']) and float(desc['unit_price']) > 0)]):
            modified_items.append(desc)

    return modified_items


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def generate_data(selection_list, start_date, end_date):
    n_row = random.randint(0, len(selection_list) - 1)
    n_units = random.choices([  1,  2,   3,   4,   5,   6,   7,   8,   9,  10],
                             [0.2,0.2,0.25,0.11,0.08,0.07,0.03,0.03,0.02,0.01])
    country = random.choices(['United States', 'China', 'Japan', 'Germany', 'India',
                              'United Kingdom', 'France', 'Canada', 'Russia', 'Italy'],
                             [0.2, 0.05, 0.05, 0.1, 0.1, 0.2, 0.1, 0.1, 0.05, 0.05])
    row = selection_list[n_row]

    data = {
        'description': row['description'],
        'unit_price': row['unit_price'],
        'units': n_units[0],
        'total': row['unit_price'] * n_units[0],
        'stock_no': row['stock_no'],
        'country': country[0],
        'InvoiceNo': str(uuid.uuid1()),
        'Date': random_date(start_date, end_date).strftime("%Y/%m/%d")
    }
    return data


def main():
    file_name = '2021'
    n_datapoints = 60000000

    items = items_details()
    print(items)

    total_units = 0
    total_price = 0

    start_date = datetime.strptime(f'{file_name}/1/1', "%Y/%m/%d")
    end_date = datetime.strptime(f'{file_name}/12/31', "%Y/%m/%d")

    with open(f'/Users/zignite/Projects/CoRise/w1/data/{file_name}.csv', 'w') as f:
        f.write(",".join(['StockCode', 'Description', 'UnitPrice', 'Quantity', 'TotalPrice', 'Country', 'InvoiceNo',
                          'Date']))
        f.write("\n")

    with open(f'/Users/zignite/Projects/CoRise/w1/data/{file_name}.csv', 'a') as f:
        for n_row in tqdm(range(n_datapoints)):
            row = generate_data(selection_list=items, start_date=start_date, end_date=end_date)
            f.write(",".join([str(row['stock_no']), str(row['description']).replace(',', ''),
                              str(row['unit_price']), str(row['units']),
                              str(row['total']), str(row['country']), str(row['InvoiceNo']), str(row['Date'])]))

            total_units += row['units']
            total_price += row['total']

            if n_row % 10000 == 0:
                print(f"Total units : {'{:,}'.format(total_units)}, Total price : {'{:,}'.format(total_price)}")

            f.write("\n")


if __name__ == '__main__':
    main()
