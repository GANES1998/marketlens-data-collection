import pandas as pd
from utils import sql_utils
from dtypes import type_defs

df = pd.read_csv('../data/sector.data', sep='\t')

rows = []

with open("../sql_generated/company_name.sql", mode='w+') as fp:

    for data in df.values:

        [symbol, name] = data[:2]

        stq_string = sql_utils.convert_row_to_sql(
            [
                type_defs.Column(name="SYMBOL", value=symbol, data_type="string"),
                type_defs.Column(name="NAME", value=name, data_type="string")
            ],
            table="COMPANY_NAME"
        )

        fp.write(
            stq_string
            + "\n"
        )

