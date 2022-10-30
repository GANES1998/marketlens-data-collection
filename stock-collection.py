import time
from typing import Dict, List
import requests
import constants
import urllib3
from sql_utils import convert_row_to_sql, convert_to_sql
from type_defs import Column
import pickle
from utils import transform_stock

sector_stock_map = pickle.load(open('data/comapanies_sector_2.pickle', 'rb'))

def get_stock_data(symbol: str, api_key: str = 'demo') -> Dict:

    url = f"https://{constants.BASE_URL}/{constants.PATH}"

    # r = ProxyRequests(f"{url}?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}")

    # print(r.get())

    data = requests.get(
        url= url,
        params = {
            'function' : 'TIME_SERIES_DAILY',
            'symbol' : symbol,
            'outputsize': 'full',
            'apikey' : api_key,
        }
    )

    print(data.url)

    return data.json()

# with open('sql_generated/stock.sql', 'a+') as fp:
#     for stock_detail in stock_details:

#         fp.write(
#             convert_row_to_sql(
#                 row=stock_detail, table='STOCK'
#             ) + "\n"
#         )

request_no = 1

existing_stocks = []

try:
    with open('existing_stock.pickle', 'rb') as handle:
        existing_stocks = pickle.load(handle)
except FileNotFoundError:
    print("No File Available for existing stock")
    pass


for sector, stocks in sector_stock_map.items():

    with open('sql_generated/stock_2.sql', 'a+') as fp:

        for stock in stocks:

            if '.' in stock:
                print(f'Invalid Stock....... [ {stock} ]. So skipping.........')
                continue

            if stock in existing_stocks:
                print(f"Stock [ {stock} ] already available. So skipping......")
                continue

            api_key = constants.API_KEYS[(request_no - 1) % (len(constants.API_KEYS))]

            print(f"Collecting data as Request [ {request_no} ] for stock [ {stock} ] of sector [ {sector} ] with API Key [ {api_key} ]")

            stock_details = transform_stock(
                get_stock_data(stock, api_key),
                default_columns = [
                    Column(
                        name= 'symbol',
                        value=stock,
                        data_type='string'
                    ),
                    Column(
                        name='stock_sector',
                        value=sector,
                        data_type='string'
                    )
                ]
            )


            for stock_detail in stock_details:

                fp.write(
                    convert_row_to_sql(
                        row=stock_detail, table='STOCK'
                    ) + "\n"
                )

            existing_stocks.append(stock)

            with open('existing_stock.pickle', 'wb+') as fp_pickle:
                pickle.dump(existing_stocks, fp_pickle)

            if request_no % 5 == 0:
                time.sleep(60)

            request_no += 1

            


