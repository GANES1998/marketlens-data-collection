import pandas as pd

import utils.utils
from dtypes import type_defs
from utils import sql_utils

stock_indices = {
    # 'S & P 500': '../csv_data/s&p500.tsv'
    # 'Dow Jones': '../csv_data/dowjones.tsv'
    # 'NASDAQ': '../csv_data/nasdaq.tsv',
    'Russel': '../csv_data/russel.tsv'
}

rows = []

with open("../sql_generated/russel_stock_index.sql", mode='w+') as fp:
    for index_name, file_path in stock_indices.items():

        df = pd.read_csv(file_path, sep='\t')

        for data in df.values:

            try:

                [date, ope, high, low, s_close, close, volume] = data

                if type(ope) == str or type(low) == str or type(close) == str or type(high) == str:
                    [ope, high, low, close, volume] = [ope.replace(",", ""), high.replace(",", ""),
                                                       low.replace(",", ""),
                                                       close.replace(",", ""),
                                                       volume.replace(",", "") if volume.strip() != "-" else "0"]

                date_reformatted = utils.utils.reformat_date(date, "%b %d, %Y", "%d-%b-%Y")

                stq_string = sql_utils.convert_row_to_sql(
                    [
                        type_defs.Column(name="DAY", value=date_reformatted, data_type="date"),
                        type_defs.Column(name="STOCK_INDEX_NAME", value=str(index_name), data_type="string"),
                        type_defs.Column(name="LOW", value=low, data_type="float"),
                        type_defs.Column(name="HIGH", value=high, data_type="float"),
                        type_defs.Column(name="OPEN", value=ope, data_type="float"),
                        type_defs.Column(name="CLOSE", value=close, data_type="float"),
                        type_defs.Column(name="VOLUME", value=volume, data_type="int")
                    ],
                    table="STOCK_INDEX"
                )

                fp.write(
                    stq_string
                    + "\n"
                )

            except Exception as e:
                print(e)
                pass

        print(f"Successfully parsed [ {file_path} ]")
