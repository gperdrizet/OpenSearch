'''Function to take a small sample of records in a dump file
and save as a separate file. Used to rapid prototyping/testing.'''

import bz2
import gzip

def run(dump: str) -> None:

    # Parse file name
    file_extension='.'.join(dump.split('.')[-2:])
    file_root='.'.join(dump.split('.')[:2])

    # Make filename for output file
    output_file_name=f'{file_root}.sample.{file_extension}'

    # Figure out what type of dump we are working with based
    # on the file extension: xml.bz2 or json.gz for CirrusSearch.
    if file_extension == 'xml.bz2':
        print(f'Sampling XML dump file')

        # Open dump file as an input stream
        input_stream=bz2.BZ2File(dump)

        # Open bz2 file in binary mode for writing
        with bz2.open(output_file_name, 'wb') as output_file:

            # Loop for 100k lines and write to output
            i=0
            while i < 100000:
                output_file.write(next(input_stream))
                i+=1

    # Handle CirrusSearch dump
    elif file_extension == 'json.gz':
        print(f'Sampling CirrusSearch dump')

        # Open dump file as an input stream
        input_stream=gzip.GzipFile(dump)
        
        # Open gzip file in binary mode for appending
        with gzip.open(output_file_name, 'ab') as output_file:

            # Loop on the input stream writing to output 10000
            # time (or 5000 articles)
            n=0

            while n < 10000:
                line=next(input_stream)
                output_file.write(line)
                n+=1

        output_file.close()

    else:
        print('Unrecognized dump file type')