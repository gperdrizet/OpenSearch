'''Functions to read data stream from dump files and send
to proper stream reader class instance'''

from __future__ import annotations
from typing import Callable
from xml import sax

def xml(
    input_stream: BZ2File, # type: ignore
    reader_instance: Callable
) -> None:

    '''Takes input data stream from file passes it to
    the reader via xml's sax parser.'''

    sax.parse(input_stream, reader_instance)


def json_lines(
    input_stream: GzipFile, # type: ignore
    reader_instance: Callable
) -> None:

    '''Takes input stream containing json lines data, passes it
    line by line to reader class instance'''

    # Loop on lines
    for line in input_stream:

        reader_instance.read_line(line)