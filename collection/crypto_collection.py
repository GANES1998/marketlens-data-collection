import time
from typing import Dict

import requests

from utils import utils, constants, sql_utils
from types.type_defs import Column


def collect_crypto_date(symbol: str, api_key: str) -> Dict:
    url = f"{constants.BASE_URL}/{constants.PATH}"

    data = requests.get(
        url,
        params={
            'function': 'DIGITAL_CURRENCY_DAILY',
            'market': 'USD',
            'symbol': symbol,
            'apikey': api_key
        }
    )

    print(data.url)

    return data.json()


crypto_currencies = ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'SOL', 'DOGE', 'BUSD', 'DOT']

request_no = 1

with open('../sql_generated/crypt_currency.sql', mode='w+') as fp:

    for crypto in crypto_currencies:

        api_key = constants.API_KEYS[(request_no - 1) % len(constants.API_KEYS)]

        crypt_data = collect_crypto_date(crypto, api_key)

        crypto_details = utils.transform_crypto(crypt_data, default_columns=[
            Column(name='currency', value=crypto, data_type='string')
        ])

        for crypto_detail in crypto_details:

            fp.write(
                sql_utils.convert_row_to_sql(row=crypto_detail, table='CRYPTO_CURRENCY')
                + '\n'
            )

        if request_no % 5 == 0:
            time.sleep(60)

        request_no += 1



