import pickle
import time

import pandas as pd
from utils import sql_utils
from dtypes import type_defs
import yfinance as yf

df = pd.read_csv('../data/sector.data', sep='\t')

rows = []

existing_symbols = []
with open('../status_dump/company_shares_status_dumps.pickle', 'rb') as fp:
    existing_symbols = pickle.load(fp)
    print(existing_symbols)

print(f"Skipping Existing symbols [ {existing_symbols} ]")

with open("../sql_generated/company_shares.sql", mode='a+') as fp:

    for data in df.values:

        try:

            [symbol, name] = data[:2]

            symbol = symbol.strip()

            if symbol in existing_symbols:
                continue

            print(symbol)

            shares = yf.Ticker(symbol).shares.values[-1][0]

            stq_string = sql_utils.convert_row_to_sql(
                [
                    type_defs.Column(name="SYMBOL", value=symbol, data_type="string"),
                    type_defs.Column(name="SHARES", value=shares, data_type="int")
                ],
                table="COMPANY_SHARES"
            )

            print(stq_string)

            time.sleep(15)

            fp.write(
                stq_string
                + "\n"
            )

            existing_symbols.append(symbol)

            with open('../status_dump/company_shares_status_dumps.pickle', 'wb+') as fpp:
                pickle.dump(existing_symbols, fpp)
                print(existing_symbols)

        except Exception as e:
            print(e)
            pass

