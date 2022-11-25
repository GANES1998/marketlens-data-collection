from typing import Dict, List

from dtypes.type_defs import Column
from utils.utils import reformat_date


def transform_treasury_data(response: Dict, default_columns: List[Column]) -> List[List[Column]]:
    data = response['data']

    rows = []

    for datum in data:
        value = datum['value']
        row = [
            Column(
                name='DAY',
                value=reformat_date(datum['date'], '%Y-%m-%d', '%d-%b-%y'),
                data_type='date'),
            Column(
                name='YIELD_RATE',
                value='NULL' if value in ['None', '.'] else float(value),
                data_type='float'
            )]

        row.extend(default_columns)

        rows.append(row)

    return rows
