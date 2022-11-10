import pickle
import time

import requests
from typing import Dict
from utils import constants
from utils.sql_utils import convert_row_to_sql
from dtypes.type_defs import Column
from utils.utils import transform_company


def collect_company_data(symbol: str, api_key: str) -> Dict:
    url = f"{constants.BASE_URL}/{constants.PATH}?"

    data = requests.get(
        url=url,
        params={
            'function': 'EARNINGS',
            'symbol': symbol,
            'apikey': api_key
        }
    )

    print(data.url)

    return data.json()


existing_stocks = []
existing_company = []


with open('../status_dump/existing_stock.pickle', 'rb') as handle:
    existing_stocks = pickle.load(handle)

try:
    with open('../status_dump/existing_company.pickle', 'rb') as handle:
        existing_company = pickle.load(handle)
except FileNotFoundError:
    print("Existing companies file not found....")

request_no = 1

with open('../sql_generated/company.sql', mode='a+') as f:
    for stock in existing_stocks:

        if stock in existing_company:
            print(f"Company [ {stock} ] already processed. So skipping.....")
            continue

        try:
            api_key = constants.API_KEYS[(request_no - 1) % len(constants.API_KEYS)]

            print(f"Collecting company details for [ {stock} ]....")

            company_data = collect_company_data(symbol=stock, api_key=api_key)

            stock_details = transform_company(company_data, [
                Column(
                    name='symbol',
                    value=stock,
                    data_type='string'
                )
            ])

            for stock_detail in stock_details:
                f.write(
                    convert_row_to_sql(
                        row=stock_detail,
                        table='COMPANY'
                    )
                    + '\n'
                )

            if request_no % 5 == 0:
                time.sleep(60)

            existing_company.append(stock)

            with open('../status_dump/existing_company.pickle', 'wb+') as handle:
                pickle.dump(existing_company, handle)

            request_no += 1

        except:
            print(f"Unable to fetch Earnings detail for [ {stock} ]")

            if request_no % 5 == 0:
                time.sleep(60)

            request_no += 1

