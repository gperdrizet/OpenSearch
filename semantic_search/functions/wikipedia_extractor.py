'''Collection of functions for Wikipedia data extractor.'''

# Standard imports
import time
import json
import pathlib
import multiprocessing as mp
from gzip import GzipFile

# PyPI imports
import h5py
import mwparserfromhell

# Internal imports
import semantic_search.configuration as config


def wikipedia_extractor(source_config: dict) -> dict:
    '''Runs text extraction and batching on CirrusSearch Wikipedia dump.'''

    # Start the extraction summary with the data from the source configuration
    extraction_summary=source_config

    # Prepare the hdf5 output
    output_file=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.BATCHED_TEXT}"
    pathlib.Path(output_file).unlink(missing_ok=True)
    output=h5py.File(output_file, 'w')
    batch_group=output.require_group('batches')

    # Open the input file stream
    gzip_data_file_path=f"{config.RAW_DATA_PATH}/{source_config['raw_data_file']}"
    file=GzipFile(gzip_data_file_path)

    # Set number of workers to one less than the CPU count and create the pool
    n_workers=mp.cpu_count() - 1
    pool=mp.Pool(processes=n_workers)

    # Counters and accumulators for batch loop
    line_count=0
    batch_count=1
    batch=[]
    batches=[]

    # Start the timer
    start_time = time.time()

    # Loop on the lines from the input file stream and accumulate batches
    for line in file:

        line_count+=1

        # Only pull text from article records (every other record is a metadata header)
        if line_count % 2 == 0:
            batch.append(line)

        # Once the batch is full, add to this round's batches and reset
        if len(batch) == source_config['batch_size']:
            batches.append(batch)
            batch=[]

        # Once we have a batch for every worker, start the round
        if len(batches) == n_workers:

            # Holder for results from workers
            worker_results=[]

            # Submit each batch to a worker
            for batch in batches:
                worker_result=pool.apply_async(extract_wikipedia_text, (batch,))
                worker_results.append(worker_result)

            # Collect the results from the workers
            results=[worker_result.get() for worker_result in worker_results]

            # Save each result as a batch in the hdf5 file
            for result in results:
                batch_group.create_dataset(str(batch_count), data=result)
                batch_count+=1

            # Stop if we have reached the user requested number of batches
            if source_config['num_batches'] != 'all':
                if source_config['num_batches'] <= batch_count:
                    break

            # Empty the batches for the next round
            batches=[]

    # Stop the timer after the last round finishes
    dT=time.time() - start_time # pylint: disable = invalid-name

    # Add some stuff the the summary
    extraction_summary['num_batches']=batch_count
    extraction_summary['run_time_seconds']=dT

    # Add some metadata to the hdf5 file
    metadata={'data_source': 'wikipedia','num_batches': batch_count}
    output.attrs.update(metadata)

    # Close the hdf5 file
    output.close()

    return extraction_summary


def extract_wikipedia_text(lines: list) -> list:
    '''Worker function to do text extraction and source specific cleaning on Wikipedia CirrusSearch
    dump source. Takes a batch of lines from file stream, returns list of text chunks.'''

    # Holder for result
    cleaned_texts=[]

    # Loop on input lines
    for line in lines:

        # Load record dictionary from JSON line
        record=json.loads(line)

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

                # Add to results
                cleaned_texts.append(source_string)

        except KeyError:
            pass

    return cleaned_texts


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
