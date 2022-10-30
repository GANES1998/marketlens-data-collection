from collections import defaultdict
import pandas as pd

from sql_utils import convert_row_to_sql
from type_defs import Column
import pickle

df = pd.read_csv(filepath_or_buffer='sector.data', sep='\t')

# Remove headers
# sectors_list = df.iloc[1:50, :].values.tolist()

# Next collect till 300 -> 51 to 3001
sectors_list = df.iloc[301:, :].values.tolist()

sectors = set()

sector_company_list = defaultdict(list)

for sector_info in sectors_list:

        symbol, industry = sector_info[0], sector_info[3]

        sector_company_list[industry].append(symbol)

        sectors.add(industry)

# with open(file='sql_generated/sector.sql', mode = 'w+') as f:
    
#     for sector in sectors:

#         f.write(
#             convert_row_to_sql(
#                 [
#                     Column(name = 'name', value = sector, data_type='string')
#                 ],
#                 table = 'SECTOR'
#             ) + '\n'
#         )

with open('data/comapanies_sector_2.pickle', 'wb+') as handle:
    pickle.dump(sector_company_list, handle)


        
    
    
        

