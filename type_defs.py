from dataclasses import dataclass
from typing import Union

@dataclass
class Column:
    name: str
    value: Union[str, float]
    data_type: str

    def get_db_value(self):

        if self.data_type == 'string':

            return f"'{self.value}'"
        elif self.data_type == 'date':
            return f"'{self.value}'"
        else:
            return str(self.value)
