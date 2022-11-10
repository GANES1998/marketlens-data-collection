import pandas as pd

from dtypes.type_defs import Column
from utils import utils, sql_utils

df = pd.read_csv('../csv_data/covid_filtered.csv')

numpy_data = df.values

n_rows, cols = numpy_data.shape

with open('../sql_generated/covid_data.sql', 'w+') as fp:
    rows = []

    for i in range(n_rows):
        row = []

        row.extend([
            Column(name='DAY', value=utils.reformat_date(numpy_data[i, 1], '%Y-%m-%d', '%d-%b-%Y'), data_type='string'),
            Column(name='NEW_CASES', value=int(numpy_data[i, 2]), data_type='int'),
            Column(name='ACTIVE_CASES', value=int(numpy_data[i, 3]), data_type='int'),
            Column(name='DEATH', value=int(numpy_data[i, 4]), data_type='int')
        ]
        )

        fp.write(
            sql_utils.convert_row_to_sql(
                row=row,
                table='COVID_DATA'
            ) + '\n'
        )
