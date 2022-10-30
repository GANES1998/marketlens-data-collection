import pandas as pd

import sql_utils
import utils
from type_defs import Column

df = pd.read_csv('csv_data/commodity.csv')

numpy_data = df.values

n_rows, cols = numpy_data.shape

with open('sql_generated/commodity.sql', 'w+') as fp:
    rows = []

    for i in range(n_rows):
        row = []

        row.extend([
            Column(name='DAY', value=utils.reformat_date(numpy_data[i, 1], '%Y-%m-%d', '%d-%b-%Y'), data_type='string'),
            Column(name='NAME', value=numpy_data[i, 0], data_type='string'),
            Column(name='Open', value=float(numpy_data[i, 2]), data_type='float'),
            Column(name='High', value=float(numpy_data[i, 3]), data_type='float'),
            Column(name='Low', value=float(numpy_data[i, 4]), data_type='float'),
            Column(name='Close', value=float(numpy_data[i, 5]), data_type='float')
        ]
        )

        fp.write(
            sql_utils.convert_row_to_sql(
                row=row,
                table='COMMODITY'
            ) + '\n'
        )


