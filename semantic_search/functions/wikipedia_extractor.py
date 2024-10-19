'''Collection of functions for Wikipedia data extractor.'''

# Standard imports
import time
import json
import pathlib
import multiprocessing as mp
from multiprocessing import Manager, Process
from gzip import GzipFile

# PyPI imports
import h5py
import mwparserfromhell

# Internal imports
import semantic_search.configuration as config


def wikipedia_extractor(source_config: dict) -> dict:
    '''Runs text extraction and batching on CirrusSearch Wikipedia dump.'''

    # Start multiprocessing manager
    manager=Manager()

    # Set-up the extraction summary as a shared variable via the multiprocessing
    # manager so that both the reader and writer processes can add some summary
    # statistics to it when they finish
    extraction_summary=manager.dict(source_config)

    # Set-up reader and writer queues to move workunit from the reader
    # process to the workers and from the workers to the writer process
    reader_queue=manager.Queue(maxsize=100)
    writer_queue=manager.Queue(maxsize=100)

    # Set-up reader and writer processes: reader gets batches of records
    # from the input file and writer takes batches for extracted text
    # from the workers and writes to file

    # Set extraction worker count based on avalible CPUs. Subtract three: one
    # for the reader and writer processes and one for the system
    n_workers=mp.cpu_count() - 3

    # IO paths
    input_file_path=f"{config.RAW_DATA_PATH}/{source_config['raw_data_file']}"
    output_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.EXTRACTED_TEXT}"

    reader_process=Process(
        target=reader,
        args=(
            input_file_path, 
            source_config['extractor_workunit_size'],
            reader_queue, 
            n_workers,
            source_config['extracted_records_target'],
            extraction_summary
        )
    )

    writer_process=Process(
        target=writer,
        args=(
            output_file_path,
            source_config['extractor_output_batch_size'],
            writer_queue,
            n_workers,
            extraction_summary
        )
    )

    # Start the extractor pool
    extractor_pool=mp.Pool(processes=n_workers)

    # Start each extraction worker
    for _ in range(n_workers):
        extractor_pool.apply_async(extract_text, (reader_queue,writer_queue,))

    # Start the reader and writer processes to begin real work, timing how long it takes.
    start_time=time.time()

    reader_process.start()
    writer_process.start()

    # Wait for the extractor pool workers to finish, then shut the pool down.
    extractor_pool.close()
    extractor_pool.join()

    # Stop the timer
    dT=time.time() - start_time

    # Clean up IO processes
    reader_process.join()
    reader_process.close()

    writer_process.join()
    writer_process.close()

    # Write some stuff to the extraction summary then recover it to a normal python dictionary
    # from the multiprocessing shared memory DictProxy object
    extraction_summary['observed_extraction_rate']=extraction_summary['input_records_extracted']/dT
    extraction_time=config.WIKIPEDIA_RECORD_COUNT / extraction_summary['observed_extraction_rate']
    extraction_summary['estimated_total_extraction_time']=extraction_time
    extraction_summary=dict(extraction_summary)

    # Close the queues and stop the manager
    manager.shutdown()

    # Finished
    return extraction_summary

def reader(
        input_file_path: str,
        extractor_workunit_size: int,
        reader_queue: mp.Queue,
        n_workers: int,
        extracted_records_target: int,
        extraction_summary: dict
) -> None:
    
    '''Reads lines from input file, collects workunits and sends them to the
    extraction workers via the reader queue. Also collects some run statistics
    via the shared memory dictionary "extraction_summary".'''

    # Open the input file stream
    file=GzipFile(input_file_path)

    # Counter and accumulator for workunits
    workunit_count=0
    workunit=[]

    # Open the input file stream
    with open(input_file_path, 'r') as file:

        # Loop until we reach EOF or the specified number of target texts, if any.
        for line_num, line in enumerate(file):

            # Add every other line to the workunit. Every other because the input file
            # has header metadata and then article content on alternating lines.
            if line_num % 2 != 0:
                workunit.append(line)

            # Once the workunit is full, send it to the extraction workers via the
            # reader queue, then clear it to start accumulating the next one.
            if len(workunit) == extractor_workunit_size:
                reader_queue.put(workunit)
                workunit_count+=1
                workunit=[]

            # If we are not processing all of the input records break when we have
            # seen the requested amount. Need to divide the line count by two here
            # because only every other line in the input file is record content
            if extracted_records_target != 'all':
                if line_num // 2 == extracted_records_target:
                    break

    # Once we have exited the line loop, either because we reached target records
    # or EOF send each worker the 'done' string.
    for _ in range(n_workers):
        reader_queue.put('done')

    # Add some information to the extraction summary and exit
    extraction_summary['input_lines_read']=line_num
    extraction_summary['input_records_extracted']=line_num // 2
    extraction_summary['extraction_workunits_processed']=workunit_count

    return


def writer(
        output_file_path,
        extractor_output_batch_size,
        writer_queue,
        n_workers,
        extraction_summary
) -> None:
    
    '''Collects workunits of extracted texts from extraction workers into batches.
    Writes them to disk in hdf5. Also collects some run statistics
    via the shared memory dictionary "extraction_summary".'''

    # Prepare the hdf5 output
    pathlib.Path(output_file_path).unlink(missing_ok=True)
    output=h5py.File(output_file_path, 'w')
    batch_group=output.require_group('batches')

    # Counter and collector for output batches and records
    output_batch_count=0
    output_record_count=0
    output_batch=[]

    # Collector for "done" signals from extraction workers. Will need to break the main loop
    # once we have seen done from each worker.
    done_count=0

    # Main loop
    while True:

        # Get the next workunit from the writer queue.
        workunit=writer_queue.get()

        # Check to see if we are done.
        if workunit == 'done':
            done_count+=1

            # Break the main loop once we have received done from each extraction worker.
            if done_count == n_workers:
                break

        # Add the extracted texts from the workunit to the output batch.
        output_batch.extend(workunit)
        print(f'Writer process received workunit, current output batch size: {len(output_batch)}')

        # If the output batch is full, write it to disk and reset for the next.
        if len(output_batch) >= extractor_output_batch_size:
            batch_group.create_dataset(str(output_batch_count), data=output_batch)
            output_record_count+=len(output_batch)
            output_batch_count+=1
            output_batch=[]

    # Once we break out of the main loop, write one last time to flush anything
    # remaining in the output batch to disk and close the output connection.
    batch_group.create_dataset(str(output_batch_count), data=output_batch)
    output_record_count+=len(output_batch)
    output_batch_count+=1
    output.close()

    # Add the batch count to the extraction summary for posterity
    extraction_summary['extracted_batches_written']=output_batch_count
    extraction_summary['extracted_texts_written']=output_record_count

    # Finished
    return


def extract_text(reader_queue: mp.Queue, writer_queue: mp.Queue) -> None:
    '''Worker function to do text extraction and source specific cleaning on 
    Wikipedia CirrusSearch dump source. Takes workunits from reader queue
    and sends extracted text to writer function for output to disk.'''

    # Loop until we receive 'done' from the reader.
    while True:

        # Get a workunit from the reader queue.
        workunit=reader_queue.get()

        # Break out of the main loop if the reader process tells us we are done.
        if workunit == 'done':
            break

        # Holder extracted text
        extracted_texts=[]

        # Loop on the input records in the workunit
        for record_json in workunit:

            # Load the record for parsing
            record=json.loads(record_json)

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
                    extracted_texts.append(source_string)

            # If we do find a key error, just skip this record
            except KeyError:
                pass
        
        # Pass the extracted texts from this workunit to the writer process
        writer_queue.put(extracted_texts)

    # Once we leave the main loop, send the "done" signal to the writer process
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
