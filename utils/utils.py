from typing import Dict, List

from dtypes.type_defs import Column
from datetime import datetime

FLOAT_COLUMNS = ['open', 'high', 'low', 'close', 'reportedEPS', 'estimatedEPS']
INT_COLUMNS = []

DATE_COLUM_NAME = 'day'

COULMN_NAME_TRANSFORMATION = {
    'estimatedEPS': 'EXPECTED_EPS',
    'reportedEPS': 'REPORTED_EPS'
}


def reformat_date(date_input: str, input_format: str, op_format: str) -> str:
    date = datetime.strptime(date_input, input_format)

    return date.strftime(op_format)


def transform_company(company_response: Dict, default_columns: List[Column]) -> List[List[Column]]:
    rows = []

    quarterly_results = company_response['quarterlyEarnings']

    for result in quarterly_results:

        row = []

        year = int(reformat_date(result['fiscalDateEnding'], '%Y-%m-%d', '%Y'))
        quarter = 1 + ((int(reformat_date(result['fiscalDateEnding'], '%Y-%m-%d', '%m')) - 1) // 3)

        row.append(
            Column(name="year", value=year, data_type="int"),
        )

        row.append(
            Column(name="quarter", value=quarter, data_type="int")
        )

        row.extend(default_columns)

        for metric, value in result.items():

            for float_col in FLOAT_COLUMNS:

                if float_col in metric:
                    col_name = COULMN_NAME_TRANSFORMATION.get(float_col, float_col)

                    row.append(
                        Column(name=col_name, value=value if value == 'None' else float(value), data_type='float')
                    )

        rows.append(row)

    return rows


def transform_price_data(price_dict: Dict, default_columns: List[Column]) -> List[List[Column]]:
    rows = []

    for date, metrics in price_dict.items():

        row = []

        row.append(
            Column(name=DATE_COLUM_NAME, value=reformat_date(date, '%Y-%m-%d', '%d-%b-%Y'), data_type='date')
        )

        row.extend(default_columns.copy())

        for metric, value in metrics.items():

            ## Skip all entries which starts with a.\s
            if 'a. ' in metric:
                continue

            for float_col in FLOAT_COLUMNS:

                if float_col in metric:
                    col_name = COULMN_NAME_TRANSFORMATION.get(float_col, float_col)

                    row.append(
                        Column(name=col_name, value=float(value), data_type='float')
                    )

            for int_col in INT_COLUMNS:

                if int_col in metric:
                    col_name = COULMN_NAME_TRANSFORMATION.get(int_col, int_col)

                    row.append(
                        Column(name=col_name, value=int(value), data_type='int')
                    )

        rows.append(row)

    return rows


def transform_stock(stock_response: Dict, default_columns: List[Column]) -> List[List[Column]]:
    return transform_price_data(stock_response['Time Series (Daily)'], default_columns=default_columns)


def transform_crypto(crypto_response: Dict, default_columns: List[Column]) -> List[List[Column]]:
    return transform_price_data(crypto_response['Time Series (Digital Currency Daily)'],
                                default_columns=default_columns)
