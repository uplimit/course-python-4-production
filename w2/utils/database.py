
# import the sqlite3 package
import sqlite3
import time
import uuid
from datetime import datetime
from pprint import pprint
import os


class DB:
    def __init__(self):
        self._connection = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                                                        "database.sqlite"), check_same_thread=False)
        self._table_name = 'processes'
        self._col_order = ['process_id', 'file_name', 'file_path', 'description', 'start_time', 'end_time', 'percentage']

        self.create_table()

    @staticmethod
    def calculate_time_taken(start_time, end_time, datetime_fmt):
        if isinstance(start_time, str) and isinstance(end_time, str):
            start_time = datetime.strptime(start_time, datetime_fmt)
            end_time = datetime.strptime(end_time, datetime_fmt)

            return (end_time - start_time).total_seconds()

        return 0

    def create_table(self):
        self._connection.execute(f'''CREATE TABLE IF NOT EXISTS 
        {self._table_name}(process_id TEXT NOT NULL, file_name TEXT DEFAULT NULL, 
                  file_path TEXT DEFAULT NULL, description TEXT DEFAULT NULL, 
                  start_time TEXT NOT NULL, end_time TEXT DEFAULT NULL, PERCENTAGE REAL DEFAULT NULL);''')

    def insert(self, process_id, start_time, file_name=None, file_path=None, description=None, end_time=None,
               percentage=None):

        columns_to_insert = ['process_id', 'start_time']
        values = ["'"+process_id+"'", "'"+start_time+"'"]

        if end_time is not None:
            columns_to_insert.append('end_time')
            values.append("'"+end_time+"'")

        if file_name is not None:
            columns_to_insert.append('file_name')
            values.append("'"+file_name+"'")

        if file_path is not None:
            columns_to_insert.append('file_path')
            values.append("'"+file_path+"'")

        if description is not None:
            columns_to_insert.append('description')
            values.append("'"+description+"'")

        if description is not None:
            columns_to_insert.append('description')
            values.append("'"+description+"'")

        if percentage is not None:
            columns_to_insert.append('percentage')
            values.append("'" + percentage + "'")

        self._connection.execute(f'''INSERT INTO {self._table_name}({",".join(columns_to_insert)}) 
                                                VALUES({",".join(values)});''')

        # commit changes to the database
        self._connection.commit()

    def read_all(self):
        data = []
        cursor = self._connection.execute(f'''SELECT {",".join(self._col_order)}
                                              FROM {self._table_name}''')
        for row in cursor.fetchall():
            row_dict = {col_name:row[ind] for ind, col_name in enumerate(self._col_order)}
            time_taken = self.calculate_time_taken(start_time=row_dict['start_time'], end_time=row_dict['end_time'],
                                                   datetime_fmt='%Y-%m-%d %H:%M:%S')
            row_dict['time_taken'] = time_taken

            data.append(row_dict)

        return data

    def update(self, process_id, end_time):

        self._connection.execute(f'''UPDATE {self._table_name} SET end_time='{end_time}'
                                     WHERE process_id='{process_id}';''')

        self._connection.commit()

    def update_percentage(self, process_id, percentage):

        self._connection.execute(f'''UPDATE {self._table_name} SET percentage={percentage}
                                     WHERE process_id='{process_id}';''')

        self._connection.commit()


def main():
    db = DB()

    example_data = [{
        'process_id': str(uuid.uuid4()),
        'start_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        'file_name': 'sample_1.csv',
        'file_path': '/usr/sample_1.csv',
        'description': 'sample'
    }, {
        'process_id': str(uuid.uuid4()),
        'start_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        'file_name': 'sample_2.csv',
        'file_path': '/usr/sample_2.csv',
        'description': 'sample'
    }, {
        'process_id': str(uuid.uuid4()),
        'start_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        'file_name': 'sample_3.csv',
        'file_path': '/usr/sample_3.csv',
        'description': 'sample'
    }]

    for each in example_data:
        db.insert(process_id=each['process_id'], start_time=each['start_time'], file_name=each['file_name'],
                  file_path=each['file_path'], description=each['description'])

    time.sleep(5)

    for each in example_data:
        db.update(process_id=each['process_id'], end_time=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

    records = db.read_all()
    pprint(records)


if __name__ == '__main__':
    main()
