from typing import Dict, List

from dtypes.type_defs import Column
from utils.utils import reformat_date


def transform_monthly_indicator_data(indicator_response: Dict, default_columns: List[Column]) -> List[List[Column]]:

    rows = []

    data = indicator_response['data']

    for datum in data:

        row = []

        year = reformat_date(datum['date'], '%Y-%m-%d', '%Y')
        month = reformat_date(datum['date'], '%Y-%m-%d', '%m')

        row.append(
            Column(
                name='YEAR',
                value=year,
                data_type='int'
            )
        )

        row.append(
            Column(
                name='MONTH',
                value=month,
                data_type='int'
            )
        )

        value = datum['value']
        row.append(
            Column(
                name='VALUE',
                value=value if value == 'None' else float(value),
                data_type='float'
            )
        )

        row.extend(default_columns)

        rows.append(row)

    return rows

def transform_quarterly_indicator_data(indicator_response: Dict, default_columns: List[Column]) -> List[List[Column]]:

    rows = []

    data = indicator_response['data']

    for datum in data:

        row = []

        year = reformat_date(datum['date'], '%Y-%m-%d', '%Y')
        month = reformat_date(datum['date'], '%Y-%m-%d', '%m')

        row.append(
            Column(
                name='YEAR',
                value=year,
                data_type='int'
            )
        )

        row.append(
            Column(
                name='QUARTER',
                value=(int(month) // 3) + 1,
                data_type='int'
            )
        )

        value = datum['value']
        row.append(
            Column(
                name='VALUE',
                value=value if value == 'None' else float(value),
                data_type='float'
            )
        )

        row.extend(default_columns)

        rows.append(row)

    return rows


