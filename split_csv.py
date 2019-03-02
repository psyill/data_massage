#!/usr/bin/env python3

# Splits a CSV file based on one of the fields.
# Reads the lines one at a time, to be able to handle large data.

# Argument parsing.
import argparse
# Automatic detection of CSV formats.
from csv import Sniffer
# Regular expressions.
import re

# First we set up an argument parser for handling arguments from the command
# line.
_parser = argparse.ArgumentParser(description='Splits CSV files based on field values')
# In this case, we want three named positional arguments. argparse can do a lot
# more, including optional arguments, but we don't need that now.
_parser.add_argument('input', type=str, help='An input CSV file')
_parser.add_argument('prefix', type=str, default='outfile_', help='Prefix for the output files')
_parser.add_argument('column', type=str, help='Index or name of the splitting field')

# By default, the argument parser parses sys.argv (if we don't provide any other
# arguments to "parse_args").
_args = _parser.parse_args()

# This is a regular expression for matching an ISO date, that is, four digits
# describing a year, followed by a dash, two digits describing a month, dash,
# two digits describing a day. Note that we limit the month and day numbers to
# those available in a year (but we don't check the entire date for
# inconsistencies, so 2011-02-31 would be considered valid).
_iso_date_re = re.compile(r'(?P<year>\d{4})-(?P<month>0\d|1[012])-(?P<day>[012]\d|3[01])')

class _EntryProcessor:
    """Processes entries, one after another, and writes each one to its
    corresponding file.
    """
    def __init__(self, field_delimiter, prefix, header, column_id):
        # Construct a template for the output file names based on the prefix
        # given. The language for specifying the format string can be found in
        # the Python library documentation.
        # N.b. we do a special trick by having double braces inside the template
        # string. This is because we are using a template for a template...
        self._outfile_template = '{}{{}}.csv'.format(prefix)
        self._header = header
        try:
            # Try to find the right column index by matching the column ID
            # against a header field.
            header_fields = header.split(sep=field_delimiter)
            column_index = header_fields.index(column_id)
        except (AttributeError, ValueError):
            # If we didn't get a header (header is None) or if the header had no
            # field matching the column ID, we end up here.
            try:
                column_index = int(column_id)
            except ValueError:
                raise Exception('No header field matching "{}" found'.format(column_id))
        # Regular expression trickery. We construct a regular expression which
        # looks for "anything but the field delimiter any number of times,
        # followed by the field delimiter" for as many times as the column
        # index, then looks for the field, which is "anything but the field
        # delimiter any number of times".
        self._field_re = re.compile(r'(?:[^{0}]*{0}){{{1}}}([^{0}]*)'.format(
            field_delimiter, column_index))
        # This will be our dictionary of open output files.
        self._out_files = {}

    def __enter__(self):
        # This special method allows us to use instances of this class in "with"
        # statements.
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # This special method allows us to use instances of this class in "with"
        # statements. It closes all open output files.
        for out_file in self._out_files.values():
            out_file.close()

    def process(self, entry):
        # Extract the interesting field from the entry. (We assume that we'll
        # always succeed in matching the field - if there's something wrong with
        # a line we will crash.)
        file_key = self._field_re.match(entry).group(1)
        # We try to match the field agains an ISO date pattern.
        iso_date = _iso_date_re.match(file_key)
        if iso_date is not None:
            # We construct a file key from the year and month of the date.
            file_key = '{}-{}'.format(iso_date.group('year'), iso_date.group('month'))
        # We construct an output file name using the template set up in the
        # initializer.
        output_file_name = self._outfile_template.format(file_key)
        try:
            # We anticipate that we have already opened this file. This will be
            # true for most of the iterations.
            out_file = self._out_files[output_file_name]
        except KeyError:
            # This is the first time we access the file.
            out_file = open(output_file_name, mode='w')
            # Write a header to the file, if available.
            if self._header is not None:
                out_file.write(self._header)
            # Put the open file in our dictionary of such.
            self._out_files[output_file_name] = out_file
        finally:
            out_file.write(entry)

# Using the "with" statement we make sure that the file we open will be closed
# when the scope of the "with" statement is exited, by whatever means.
with open(_args.input, mode='r', newline='') as input_file:
    # We read in the first line and...
    first_line = input_file.readline()

    # ... check if we can detect the CSV format from it.
    sniffer = Sniffer()
    dialect = sniffer.sniff(first_line, delimiters=',;\t')

    # We also check if the first line is a header.
    header = None
    if sniffer.has_header(first_line):
        header = first_line
    else:
        # Nope, no header, this was ordinary data. Reset the file position so we
        # don't skip the first line by accident.
        input_file.seek(0)
    # Once again, using the "with" statement we make sure that the entry
    # processor closes all its open files before we leave.
    with _EntryProcessor(dialect.delimiter, _args.prefix, header, _args.column) as processor:
        for line in input_file:
            processor.process(line)
