#!/usr/bin/env python3

from pathlib import Path
from typing import FrozenSet, TypeVar
import sys

import pandas as pd
from pandas.io.sas.sas7bdat import SAS7BDATReader as SasReader
from pandas.io.stata import StataWriterUTF8 as StataWriter

T = TypeVar('T')

try:
    _input_file_path: Path = Path(sys.argv[1]).absolute()
except IndexError:
    sys.exit('Missing input file parameter')

_output_file_path: Path = _input_file_path.with_suffix('.dta')
_important_column: str = 'FOO'
_important_value: str = 'foo'
_wanted_columns: FrozenSet[str] = frozenset(('BAR',))
_chunksize: int = 1000000


def _is_interesting(row: pd.Series) -> bool:
    return _important_value == row[[_important_column]]


class ProgressCounter:
    def __init__(self, name: str, total: int):
        self._name = name
        self._progress: int = 0
        self._count: int = 0
        self._threshold: int = int(total / 100)
        self._print_progress()

    def __call__(self, *args: T) -> T:
        self._count += 1
        if self._threshold <= self._count:
            self._progress += 1
            self._count = 0
            self._print_progress()
        return args

    def _print_progress(self):
        print('{} progress: {} %'.format(self._name, self._progress), file=sys.stderr)


_sas_reader = SasReader(
    _input_file_path,
    encoding='latin-1',
    chunksize=_chunksize
)

_number_of_observations = _sas_reader.row_count
print('Input has {} observations'.format(_number_of_observations), file=sys.stderr)
_extracted_columns = list(frozenset(_sas_reader.column_names).intersection(_wanted_columns))
_input_counter = ProgressCounter('input', _number_of_observations)

_input_row_generator = (_input_counter(row)
                        for chunk in _sas_reader
                        for index, row in chunk)

_output_row_generator = (row[_extracted_columns]
                         for row in filter(_is_interesting, _input_row_generator))

_result: pd.DataFrame = pd.DataFrame(_input_row_generator)
StataWriter(
    _output_file_path,
    _result,
    write_index=False,
    version=118
).write_file()
