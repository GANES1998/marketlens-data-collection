from typing import List
from string import Template

from type_defs import Column

SQL_TEMPLATE = "INSERT INTO $Table ($ColumnNames) values ($Values);"


def convert_to_sql(rows: List[List[Column]], table: str) -> List[str]:

    sqls = []

    first_row = rows[0]

    row_names = list(map(lambda x:x.name, first_row))
    values = list(map(lambda x:x.get_db_value(), first_row))

    sql = Template(SQL_TEMPLATE).substitute({
        'Table' : table,
        'ColumnNames': (', '.join(row_names)).upper(),
        'Values': ', '.join(values)
    })

    print(sql)

def convert_row_to_sql(row: List[Column], table: str) -> str:

    row_names = list(map(lambda x:x.name, row))
    values = list(map(lambda x:x.get_db_value(), row))

    sql = Template(SQL_TEMPLATE).substitute({
        'Table' : table,
        'ColumnNames': (', '.join(row_names)).upper(),
        'Values': ', '.join(values)
    })

    return sql



