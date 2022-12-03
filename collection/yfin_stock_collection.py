import pickle
import time

import pandas as pd

import os
from utils import sql_utils
from dtypes import type_defs
import yfinance as yf

from utils.sql_utils import convert_row_to_sql

df = pd.read_csv('../data/sector.data', sep='\t')

rows = []

existing_symbols = []
if os.path.exists("../status_dump/yfin_stock_status_dumps.pickle"):
    with open('../status_dump/yfin_stock_status_dumps.pickle', 'rb') as fp:
        existing_symbols = pickle.load(fp)
        print(existing_symbols)

print(f"Skipping Existing symbols [ {existing_symbols} ]")

with open("../sql_generated/yfin_stock.sql", mode='a+') as fp:
    for data in df.values:

        try:

            [symbol, name, random, sector] = data[:4]

            symbol = symbol.strip()

            if symbol in existing_symbols:
                continue

            print(symbol)

            history_df = yf.Ticker(symbol).history(start="2016-01-01", end="2022-11-24")

            history_df.index = history_df.index.strftime('%d-%b-%Y')

            for i, stock_data in enumerate(history_df.values):
                day = history_df.index[i]

                ope, high, low, close = stock_data[:4]

                stq_string = sql_utils.convert_row_to_sql(
                    [
                        type_defs.Column(name="DAY", value=day, data_type="string"),
                        type_defs.Column(name="SYMBOL", value=symbol, data_type="string"),
                        type_defs.Column(name="LOW", value=low, data_type="float"),
                        type_defs.Column(name="HIGH", value=high, data_type="float"),
                        type_defs.Column(name="OPEN", value=ope, data_type="float"),
                        type_defs.Column(name="CLOSE", value=close, data_type="float"),
                        type_defs.Column(name="STOCK_SECTOR", value=sector, data_type="string")
                    ],
                    table="STOCK"
                )

                x = sql_utils.convert_row_to_sql(
                    row=[

                    ],
                    table="STOCK"
                )

                fp.write(
                    stq_string
                    + "\n"
                )

            existing_symbols.append(symbol)

            time.sleep(15)

            with open('../status_dump/yfin_stock_status_dumps.pickle', 'wb+') as fpp:
                pickle.dump(existing_symbols, fpp)
                print(existing_symbols)

        except Exception as e:
            print(e)
            pass
