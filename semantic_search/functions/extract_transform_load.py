'''Data pipeline functions, meant to be called by Luigi tasks.'''

# Standard imports
import time
import json
import pathlib
from gzip import GzipFile

# PyPI imports
import h5py
import mwparserfromhell

# Internal imports
import semantic_search.configuration as config

def extract_data(data_source: str) -> dict:
    '''Wrapper function to call correct task specific data
    extractor function'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Pick the extractor function to run based on the data source configuration
    extractor_function=globals()[source_config['extractor_function']]

    # Run the extraction
    extraction_summary=extractor_function(source_config)

    return extraction_summary

def wikipedia_extractor(source_config: dict) -> dict:
    '''Runs text extraction and batching on CirrusSearch Wikipedia dump.'''

    # Start the extraction summary with the data from the source configuration
    extraction_summary=source_config

    # Prepare the hdf5 output
    output_file=f"{config.DATA_PATH}/{source_config['output_data_dir']}/{config.BATCHED_TEXT}"
    pathlib.Path(output_file).unlink(missing_ok=True)
    output=h5py.File(output_file, 'w')
    batch_group=output.require_group('batches')

    # Open the input file stream
    gzip_data_file_path=f"{config.RAW_DATA_PATH}/{source_config['raw_data_file']}"
    file=GzipFile(gzip_data_file_path)

    # Loop on the line from the input file stream and accumulate batches
    line_count=0
    batch_count=0
    batch=[]

    start_time = time.time()

    for line in file:

        line_count+=1

        # Only pull text from article records (every other record is a metadata header)
        if line_count % 2 == 0:
            record=json.loads(line)

            text=record['source_text']

            # Convert source string to wikicode
            wikicode=mwparserfromhell.parse(text)

            # Strip garbage out of wikicode source
            source_string=wikicode.strip_code(
                normalize=True,
                collapse=True,
                keep_template_params=False
            )

            # Remove extra sections from the end of the document
            source_string=remove_extra_sections(source_string)

            # Do some string replacements
            source_string=fix_bad_symbols(source_string)

            # Clean up newlines
            source_string=clean_newlines(source_string)

            # Get rid of image thumbnail lines and leading spaces
            source_string=remove_thumbnails(source_string)

            batch.append(source_string)

        # Once the batch is full, save it and reset
        if len(batch) == source_config['batch_size']:
            batch_group.create_dataset(str(batch_count), data=batch)
            batch_count+=1
            batch=[]

    # Once we have looped trough the file, save any remaining records
    # as one last batch
    if len(batch) != 0:
        batch_group[str(batch_count)]=batch

    dT=time.time() - start_time

    # Add some stuff the the summary
    extraction_summary['num_batches']=batch_count
    extraction_summary['run_time_seconds']=dT

    # Add some metadata to the hdf5 file
    metadata={'data_source': 'wikipedia','num_batches': batch_count}
    output.attrs.update(metadata)

    # Close the hdf5 file
    output.close()

    return extraction_summary


def fix_bad_symbols(source_string: str) -> str:
    '''Fixes some weird punctuation and symbols left over after
    code is stripped by mwparserfromhell'''

    source_string=source_string.replace('–', '-')
    source_string=source_string.replace('(/', '(')
    source_string=source_string.replace('/)', ')')
    source_string=source_string.replace('(, ', '(')
    source_string=source_string.replace('( , ; ', '(')
    source_string=source_string.replace(' ', ' ')
    source_string=source_string.replace('′', '`')
    source_string=source_string.replace('(: ', '(')
    source_string=source_string.replace('(; ', '(')
    source_string=source_string.replace('( ', '(')
    source_string=source_string.replace(' )', ')')
    source_string=source_string.replace('皖', '')
    source_string=source_string.replace('()', '')
    source_string=source_string.replace('(;)', '')
    source_string=source_string.replace(' ; ', '; ')
    source_string=source_string.replace('(,', '(')
    source_string=source_string.replace(',)', ')')
    source_string=source_string.replace(',),', ',')
    source_string=source_string.replace(',“', ', "')
    source_string=source_string.replace('( ;)', '')
    source_string=source_string.replace('(;', '(')
    source_string=source_string.replace(' .', '.')
    source_string=source_string.replace(';;', ';')
    source_string=source_string.replace(';\n', '\n')
    source_string=source_string.replace(' ,', ',')
    source_string=source_string.replace(',,', ',')
    source_string=source_string.replace('−', '-')
    source_string=source_string.replace('۝ ', '')
    source_string=source_string.replace('۝', '')
    source_string=source_string.replace("\'", "'")


    # Need to do this one last, some of the above
    # replace-with-nothings leave double spaces
    source_string=source_string.replace('  ', ' ')

    return source_string


def clean_newlines(source_string: str) -> str:
    '''Fixes up some issues with multiple newlines'''

    source_string=source_string.replace(' \n', '\n')
    source_string=source_string.replace('\n\n\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n', '\n\n')
    source_string=source_string.replace('\n\n\n', '\n')
    source_string=source_string.replace('\n\n\n', '\n\n')

    return source_string


def remove_thumbnails(source_string: str) -> str:
    '''Removes thumbnail descriptor lines and cleans up any
    lines with leading spaces'''

    # Empty list for cleaned lines
    cleaned_source_array=[]

    # Split the source string on newlines
    source_array=source_string.split('\n')

    # Loop on the line array
    for line in source_array:

        # Only take lines that don't contain leftover HTML stuff
        if 'thumb|' in line:
            pass

        elif 'scope="' in line:
            pass

        elif 'rowspan="' in line:
            pass

        elif 'style="' in line:
            pass

        # If we don't find any of the above, process the line
        else:

            # Check for lines that are not blank but start with space
            # or other garbage
            if len(line) > 1:

                if line[0] == ' ':
                    line=line[1:]

                if line[:2] == '| ':
                    line=line[2:]

                if line[:2] == '! ':
                    line=line[2:]

                if line[:2] == '! ':
                    line=line[2:]

                if line[:2] == '|-':
                    line=line[2:]

                if line[:2] == '|}':
                    line=line[2:]

            # Add the cleaned line to the result
            cleaned_source_array.append(line)

    # Join the cleaned lines back to a string
    source_string='\n'.join(cleaned_source_array)

    return source_string


def remove_extra_sections(source_string: str) -> str:
    '''Remove extra sections from the end of the document by splitting 
    on common headings only keeping the stuff before the split point'''

    source_string=source_string.split('See also')[0]
    source_string=source_string.split('References')[0]
    source_string=source_string.split('External links')[0]
    source_string=source_string.split('Notes')[0]

    return source_string
