import time
from typing import Dict

import requests

from dtypes.type_defs import Column
from utils import constants
from utils.indicator import transform_monthly_indicator_data, transform_quarterly_indicator_data
from utils.sql_utils import convert_row_to_sql


def collect_indicator_data(function: str, api_key: str) -> Dict:
    url = f"{constants.BASE_URL}/{constants.PATH}?"

    data = requests.get(
        url=url,
        params={
            'function': function,
            'interval': 'quarterly',
            'apikey': api_key
        }
    )

    print(data.url)

    return data.json()

request_no = 1

MONTHLY_INDICATORS = ['REAL_GDP_PER_CAPITA', 'REAL_GDP']

with open('../sql_generated/quarterly_indicator.sql', mode='w+') as f:

    for indicator in MONTHLY_INDICATORS:

        API_KEY = constants.API_KEYS[(request_no - 1) % (len(constants.API_KEYS))]

        response = collect_indicator_data(function=indicator, api_key=API_KEY)

        indicator_details = transform_quarterly_indicator_data(
            indicator_response=response,
            default_columns=[
                Column(
                    name='INDICATOR',
                    value=indicator,
                    data_type='string'
                )
            ]
        )

        for indicator_detail in indicator_details:

            f.write(
                convert_row_to_sql(
                    indicator_detail,
                    table='QUARTERLY_INDICATOR'
                ) + '\n'
            )

        if request_no % 5 == 0:
            time.sleep(60)

        print(f"Successfully handled ECON INDICATOR [ {indicator} ]")

