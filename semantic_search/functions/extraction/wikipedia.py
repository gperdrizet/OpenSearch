'''Collection of functions for Wikipedia data extractor.'''

# Standard imports
import json
import multiprocessing as mp

# PyPI imports
import mwparserfromhell


def wikipedia_reader(
        input_file_path: str,
        reader_queue: mp.Queue,
        n_workers: int,
        target_records: int,
        summary: dict
) -> None:
    
    '''Reads lines from input file, and sends them to the records to 
    the workers via the reader queue. Also collects some run statistics
    via the shared memory dictionary "summary".'''

    # Counter to track the number of records we have sent to the workers
    record_count=0

    # Open the input file stream
    with open(input_file_path, 'r') as file:

        # Loop until we reach EOF or the specified number of records, if any.
        for line_num, line in enumerate(file):

            # Send every other line to the workers - in the Wikipedia input file
            # the lines alternate between a metadata header and the article content
            if line_num % 2 != 0:
                reader_queue.put(line)
                record_count+=1

            # If we are not processing all of the input records break when we have
            # sent the requested amount to the workers
            if target_records != 'all':
                if record_count == target_records:
                    break

    # Once we have exited the line loop, either because we submitted enough records
    # to satisfy the record target, or we ran out out input, send each worker the
    # done signal.
    for _ in range(n_workers):
        reader_queue.put('done')

    # Add some information to the summary and exit
    summary['Input records read']=record_count

    return


def wikipedia_extractor(reader_queue: mp.Queue, writer_queue: mp.Queue, worker) -> None:
    '''Worker function to do text extraction and source specific cleaning on 
    Wikipedia CirrusSearch dump source. Takes records from reader queue
    and sends extracted text to writer process for output to disk.'''

    print(f'Worker {worker} starting Wikipedia extraction.')

    # Loop until we receive 'done' from the reader.
    while True:

        # Get a record from the reader queue.
        record=reader_queue.get()

        # Break out of the main loop if the reader process tells us we are done.
        if record == 'done':
            break

        # Load the record into a dictionary for parsing
        record=json.loads(record)

        # Get the text from the record, catching key error
        # in case this record doesn't have text for some reason
        try:

            # Only parse namespace 0 articles which are not disambiguation
            if record['namespace'] == 0 and 'Disambiguation pages' not in record['category']:

                # Convert source string to wikicode
                wikicode=mwparserfromhell.parse(record['source_text'])

                # Strip garbage out of wikicode source
                source_string=wikicode.strip_code(
                    normalize=True,
                    collapse=True,
                    keep_template_params=False
                )

                # Remove extra sections from the end of the document
                source_string=remove_extra_sections(source_string)

                # Get rid of image thumbnail lines and leading spaces
                source_string=remove_thumbnails(source_string)

                # Pass the extracted text from this to the writer process
                writer_queue.put(source_string)

        # If we do find a key error, just skip this record
        except KeyError:
            pass

    # Once we leave the main loop, send the 'done' signal to the writer process
    writer_queue.put('done')

    # Finish
    return


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
