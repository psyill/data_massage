#!/usr/bin/env python3

# Splits a CSV file based on one of the fields.
# Reads the lines one at a time, to be able to handle large data.

import argparse
from csv import Sniffer
import re

_parser = argparse.ArgumentParser(description='Splits CSV files based on field values')
_parser.add_argument('input', type=str, help='An input CSV file')
_parser.add_argument('prefix', type=str, default='outfile_', help='Prefix for the output files')
_parser.add_argument('column', type=str, help='Index or name of the splitting field')

_args = _parser.parse_args()

_iso_date_re = re.compile(r'(?P<year>\d{4})-(?P<month>0\d|1[012])-(?P<day>[012]\d|3[01])')

class _EntryProcessor:
    def __init__(self, field_delimiter, prefix, header, column_id):
        self._outfile_template = '{}{{}}.csv'.format(prefix)
        self._header = header
        try:
            # Try to find the right column index by matching the column ID
            # against a header field.
            header_fields = header.split(sep=field_delimiter)
            column_index = header_fields.index(column_id)
        except (AttributeError, ValueError):
            try:
                column_index = int(column_id)
            except ValueError:
                raise Exception('No header field matching "{}" found'.format(column_id))
        self._field_re = re.compile(r'(?:[^{0}]*{0}){{{1}}}([^{0}]*)'.format(
            field_delimiter, column_index))
        self._out_files = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for out_file in self._out_files.values():
            out_file.close()

    def process(self, entry):
        file_key = self._field_re.match(entry).group(1)
        iso_date = _iso_date_re.match(file_key)
        if iso_date is not None:
            file_key = '{}-{}'.format(iso_date.group('year'), iso_date.group('month'))
        output_file_name = self._outfile_template.format(file_key)
        try:
            out_file = self._out_files[output_file_name]
        except KeyError:
            out_file = open(output_file_name, mode='w')
            if self._header is not None:
                out_file.write(self._header)
            self._out_files[output_file_name] = out_file
        finally:
            out_file.write(entry)

with open(_args.input, mode='r', newline='') as input_file:
    first_line = input_file.readline()

    sniffer = Sniffer()
    dialect = sniffer.sniff(first_line, delimiters=',;\t')

    header = None
    if sniffer.has_header(first_line):
        header = first_line
    else:
        input_file.seek(0)
    with _EntryProcessor(dialect.delimiter, _args.prefix, header, _args.column) as processor:
        for line in input_file:
            processor.process(line)
