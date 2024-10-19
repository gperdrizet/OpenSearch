'''Functions for data cleaning and chunking. Meant to be
run by multiprocessing pool workers'''

# Standard imports
import multiprocessing as mp

# PyPI imports
import h5py
from semantic_text_splitter import TextSplitter # pylint: disable = no-name-in-module
from tokenizers import Tokenizer

# Internal imports
import semantic_search.configuration as config

def submit_batches(
    n_workers: int,
    batches: list,
    output_batch_group: h5py._hl.group.Group,
    batch_count: int,
    chunk_count: int,
) -> int:

    '''Takes batches list and current batch count, submits batches to worker pool for parsing.
    Returns updated batch count after receiving and saving worker results.'''

    # Holder for results from workers
    worker_results=[]

    # Start the pool
    pool=mp.Pool(processes=n_workers)

    # Holder for results from workers
    worker_results=[]

    # Submit each batch to a worker
    for batch in batches:
        worker_result=pool.apply_async(clean_and_chunk, (batch,))
        worker_results.append(worker_result)

    # Collect the results from the workers
    results=[worker_result.get() for worker_result in worker_results]

    # Save each result as a batch in the hdf5 file
    for result in results:
        output_batch_group.create_dataset(str(batch_count), data=result)
        batch_count+=1
        chunk_count+=len(result)

    return batch_count, chunk_count


def clean_and_chunk(texts: list) -> list:
    '''Cleans and chunks batch of text, returns list of chunks'''

    # Fire up the semantic chunk splitter
    tokenizer=Tokenizer.from_pretrained(config.TOKENIZER_NAME)
    splitter=TextSplitter.from_huggingface_tokenizer(tokenizer, config.MAX_TOKENS)

    # Holder for results
    transformed_text=[]

    # Loop on texts in the batch
    for text in texts:

        # Do some string replacements
        text=fix_bad_symbols(text)

        # Clean up newlines
        text=clean_newlines(text)

        # Split the text into chunks
        chunks=splitter.chunks(text)

        # Add the chunks to the result
        transformed_text.extend(chunks)

    return transformed_text


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
