#!/usr/bin/env python3

import argparse
from pathlib import Path
import subprocess
from typing import Optional, Sequence


def _to_existing_directory(path: str) -> Path:
    """Takes a string path and converts it to a Path instance.
    Checks that the path is an existing directory.

    :param path:
    :return:
    """
    result: Path = Path(path)
    if not result.is_dir():
        raise argparse.ArgumentTypeError('"{}" is not an existing directory'.format(path))
    return result


# TODO: Replace this with the external program you want to run.
_external_program: str = 'echo'

_parser = argparse.ArgumentParser(description='Runs {} with the supplied parameters'.format(_external_program))
_parser.add_argument('input_directory',
                     type=_to_existing_directory,
                     # TODO: Change the help text for the argument.
                     help='Will be searched for the input files. Must contain both splurb files and the frotz.xml description.')
_parser.add_argument('output_directory',
                     type=_to_existing_directory,
                     # TODO: Change the help text for the argument.
                     help='Output files will be created here.')
_parser.add_argument(
    '-v', '--verbose',
    action='store_true',
    help='shows the printed output from the external program')
# TODO: Add more arguments to the parser as needed.

_arguments = _parser.parse_args()

_message_stream: Optional[int] = None if _arguments.verbose else subprocess.DEVNULL
# TODO: Replace this with the correct syntax for running the external program with its arguments.
_external_command: Sequence[str] = (
    _external_program,
    _arguments.input_directory,
    _arguments.output_directory

)
subprocess.run(_external_command, check=True, stderr=_message_stream, stdout=_message_stream)
