import pandas as pd

df = pd.read_csv('../csv_data/s&p500.tsv', sep='\t')

print(df.tail(10))