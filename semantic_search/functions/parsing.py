'''Functions for data cleaning and chunking. Meant to be
run by multiprocessing pool workers'''

# Standard imports
import pathlib
import multiprocessing as mp

# PyPI imports
import h5py
from semantic_text_splitter import TextSplitter
from tokenizers import Tokenizer

# Internal imports
import semantic_search.configuration as config

def reader(
        input_file_path: str,
        reader_queue: mp.Queue,
        n_workers: int,
        parse_summary: dict
) -> None:
    
    '''Reads batches from extracted text file and sends them to the
    parse workers via the reader queue. Also collects some run statistics
    via the shared memory dictionary "parse_summary".'''

    # Open the input file connection
    input_data=h5py.File(input_file_path, 'r')

    # Counter for input batches and records
    batch_count=0
    text_count=0

    # Loop on the batches
    for batch_num in input_data['batches']:

        # Grab the batch from the hdf5 connection and send it to the parse workers
        batch=list(input_data[f'batches/{batch_num}'])
        reader_queue.put(batch)
        text_count+=len(batch)
        batch_count+=1

    # Once we have sent all of the batches, send each worker a 'done' signal
    for _ in range(n_workers):
        reader_queue.put('done')

    # Add the input batch count to the parse summary
    parse_summary['parse_input_batches']=batch_count
    parse_summary['parse_input_texts']=text_count

    # Finished
    return


def writer(
        output_file_path,
        writer_queue,
        n_workers,
        parse_summary
) -> None:
    
    '''Collects batches of parsed texts from parse workers.
    Writes them to disk in hdf5. Also collects some run statistics
    via the shared memory dictionary "parse_summary".'''

    # Prepare the hdf5 output
    pathlib.Path(output_file_path).unlink(missing_ok=True)
    output=h5py.File(output_file_path, 'w')
    output_batch_group=output.require_group('batches')

    # Counter and collector for output batches and records
    output_batch_count=0
    output_record_count=0

    # Collector for "done" signals from extraction workers. Will need to break the main loop
    # once we have seen done from each worker.
    done_count=0

    # Main loop
    while True:

        # Get the next batch from the writer queue.
        batch=writer_queue.get()

        # Check to see if we are done.
        if batch == 'done':
            done_count+=1

            # Break the main loop once we have received done from each parse worker.
            if done_count == n_workers:
                break

        elif batch != 'done':
            # Write the batch to output hdf5 
            output_batch_group.create_dataset(str(output_batch_count), data=batch)
            output_batch_count+=1
            output_record_count+=len(batch)

    # Close the output connection
    output.close()

    # Add the batch count to the parse summary for posterity
    parse_summary['parsed_batches_written']=output_batch_count
    parse_summary['parsed_texts_written']=output_record_count

    # Finished
    return


def parse_text(reader_queue: mp.Queue, writer_queue: mp.Queue) -> list:
    '''Takes batch of text from reader queue. Cleans and semantically splits texts in batch,
    sends batch of parsed texts to writer process.'''

    # Fire up the semantic chunk splitter
    tokenizer=Tokenizer.from_pretrained(config.TOKENIZER_NAME)
    splitter=TextSplitter.from_huggingface_tokenizer(tokenizer, config.MAX_TOKENS)

    # Counter for batches sent to writer
    output_batch_count=0

    # Main loop
    while True:
        
        # Get the next batch from the reader queue
        input_batch=reader_queue.get()

        # Break out of the main loop when the reader process tells us we are done
        if input_batch == 'done':
            break

        # Accumulator for parsed texts
        output_batch=[]

        for text in input_batch:

            # Strings come out of hdf5 as bytes
            text=text.decode('utf-8')

            # Do some string replacements
            text=fix_bad_symbols(text)

            # Clean up newlines
            text=clean_newlines(text)

            # Split the text into chunks
            chunks=splitter.chunks(text)

            # Add the chunks to the result
            output_batch.extend(chunks)

        # Send the output batch to the writer process
        writer_queue.put(output_batch)
        output_batch_count+=1

    # When we break out of the main loop due to a "done" signal from the 
    # reader process, send the same done signal along to the writer
    # process
    writer_queue.put('done')

    # Finished
    return


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
