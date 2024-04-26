'''Function to take a small sample of records in a dump file
and save as a separate file. Used to rapid prototyping/testing.'''

from bz2 import BZ2File
from gzip import GzipFile

def run(dump: str) -> None:

    # Figure out what type of dump we are working with based
    # on the file extension: xml.bz2 or json.gz for CirrusSearch.
    
    if '.'.join(dump.split('.')[-2:]) == 'xml.bz2':
        print(f'Sampling XML dump file')

    # Handle CirrusSearch dump
    elif '.'.join(dump.split('.')[-2:]) == 'json.gz':
        print(f'Sampling CirrusSearch dump')

        # Empty list to hold lines for output
        output_list = []

        # Open dump file as an input stream
        input_stream=GzipFile(dump)

        # Count lines, stopping when we have 10000 (or 5000 articles)
        n=0

        while n < 10000:
            output_list.append(next(input_stream))
            n+=1

    else:
        print('Unrecognized dump file type')