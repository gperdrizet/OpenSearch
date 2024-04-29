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

        # Open bz2 file in binary mode for write
        with bz2.open(output_file_name, 'wb') as output_file:

            # Loop until we have found 5000 page tags
            # and then come to the next closing page tag
            i=0
            while True:

                # Get the next line
                line=next(input_stream)

                # Write it to output
                output_file.write(line)

                # Count page tags as a proxy for number of
                # articles
                if '<page>' in line.decode():
                    i+=1

                # Once we have seen 5000 page tags and have
                # come to a closing page tag, stop
                if i>=1000 and '</page>' in line.decode():

                    # Add a closing mediawiki tag so the XML
                    # tree is not incomplete
                    output_file.write('</mediawiki>'.encode())
                    
                    # Stop the loop
                    break


            output_file.close()

    # Handle CirrusSearch dump
    elif file_extension == 'json.gz':
        print(f'Sampling CirrusSearch dump')

        # Open dump file as an input stream
        input_stream=gzip.GzipFile(dump)
        
        # Open gzip file in binary mode for write
        with gzip.open(output_file_name, 'wb') as output_file:

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