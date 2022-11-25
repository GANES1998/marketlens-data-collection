import time
from typing import Dict

import requests

from dtypes.type_defs import Column
from utils import constants
from utils.sql_utils import convert_row_to_sql
from utils.treasury import transform_treasury_data


def collect_treasury_yield(maturity: str, api_key: str) -> Dict:
    url = f"{constants.BASE_URL}/{constants.PATH}?"

    data = requests.get(
        url=url,
        params={
            'function': 'TREASURY_YIELD',
            'interval': 'daily',
            'maturity': maturity,
            'apikey': api_key
        }
    )

    print(data.url)

    return data.json()


request_no = 1

MATURITIES = ['2', '5', '7', '10', '30']

with open('../sql_generated/treasury_yield.sql', mode='w+') as f:
    for maturity in MATURITIES:

        API_KEY = constants.API_KEYS[(request_no - 1) % (len(constants.API_KEYS))]

        response = collect_treasury_yield(maturity=maturity + 'year', api_key=API_KEY)

        treasury_details = transform_treasury_data(response, default_columns=[
            Column(
                name='TENURE',
                value=int(maturity),
                data_type='int'
            )
        ])

        for treasury_detail in treasury_details:
            f.write(
                convert_row_to_sql(
                    treasury_detail,
                    table='TREASURY_BOND'
                ) + '\n'
            )

        if request_no % 5 == 0:
            time.sleep(60)

        request_no += 1

        print(f"Successfully handled Treasury bond for [ {maturity} ] years maturity")
