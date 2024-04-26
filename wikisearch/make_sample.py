'''Function to take a small sample of records in a dump file
and save as a separate file. Used to rapid prototyping/testing.'''

def run(dump: str) -> None:

    # Figure out what type of dump we are working with based
    # on the file extension: xml.bz2 or json.gz for CirrusSearch.
    
    if '.'.join(dump.split('.')[-2:]) == 'xml.bz2':
        print(f'Sampling XML dump file')

    elif '.'.join(dump.split('.')[-2:]) == 'json.gz':
        print(f'Sampling CirrusSearch dump')

    else:
        print('Unrecognized dump file type')