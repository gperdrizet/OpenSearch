'''Collection of functions for notebooks.'''

# Standard imports
import multiprocessing as mp

# PyPI imports
import h5py
import torch
from transformers import AutoTokenizer, AutoModel
from opensearchpy import OpenSearch # pylint: disable = import-error

# Internal imports
import semantic_search.configuration as config

############################################################
# Wikipedia data cleaning functions ########################
############################################################

def remove_extra_sections(source_string: str) -> str:
    '''Remove extra sections from the end of the document by splitting 
    on common headings only keeping the stuff before the split point'''

    source_string=source_string.split('See also')[0]
    source_string=source_string.split('References')[0]
    source_string=source_string.split('External links')[0]
    source_string=source_string.split('Notes')[0]

    return source_string


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

############################################################
# OpenSearch functions #####################################
############################################################

def start_client() -> OpenSearch:

    '''Fires up the OpenSearch client'''

    # Set host and port
    host='localhost'
    port=9200

    # Create the client with SSL/TLS and hostname verification disabled.
    client=OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_compress=False,
        timeout=30,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    return client

def initialize_index(index_name: str, index_body: dict) -> None:

    '''Set-up OpenSearch index. Deletes index if it already exists
    at run start. Creates new index for run.'''

    client=start_client()

    # Delete the index we are trying to create if it exists
    if client.indices.exists(index=index_name):
        _=client.indices.delete(index=index_name)

    # Create the target index if it does not exist
    if client.indices.exists(index=index_name) is False:
        _=client.indices.create(index_name, body=index_body)

    # Close client
    client.close()


############################################################
# GPU embedding functions ##################################
############################################################

def submit_batches(
    n_workers: int,
    batches: list,
    output_batch_group: h5py._hl.group.Group,
    batch_count: int
) -> int:

    '''Takes batches list and current batch count, submits batches to worker pool for embedding.
    Returns updated batch count after receiving and saving worker results.'''

    # Holder for results from workers
    worker_results=[]

    # Start the pool
    pool=mp.Pool(processes=n_workers)

    # Holder for results from workers
    worker_results=[]

    # Submit each batch to a worker
    for batch, gpu in zip(batches, config.WORKER_GPUS):
        worker_result=pool.apply_async(calculate_embeddings, (batch,gpu,))
        worker_results.append(worker_result)

    # Collect the results from the workers
    results=[worker_result.get() for worker_result in worker_results]

    # Save each result as a batch in the hdf5 file
    for result in results:
        output_batch_group.create_dataset(str(batch_count), data=result)
        batch_count+=1

    return batch_count


def calculate_embeddings(batch: list, gpu: str) -> list:
    '''Takes batch of text and gpu identifier, calculates and
    returns text embeddings.'''

    # Load the model and tokenizer
    tokenizer=AutoTokenizer.from_pretrained(config.EMBEDDING_MODEL)
    model=AutoModel.from_pretrained(config.EMBEDDING_MODEL, device_map=gpu)

    result=[]

    # Loop on aggregate batch by generating chunks of EMBEDDING_BATCH_SIZE
    for text in yield_batches(batch):

        # Tokenize the texts
        encoded_input=tokenizer(
            text,
            padding=True,
            truncation=True,
            return_tensors='pt'
        ).to(gpu)

        # Compute token embeddings
        with torch.no_grad():
            model_output=model(**encoded_input, return_dict=True)

        # Perform pooling
        embeddings=model_output.last_hidden_state[:,0]

        # Collect the result
        result.extend(embeddings.tolist())

    # Return the embeddings as list
    return result


def yield_batches(batch: list):
    '''Yields individual batches from master batch sent to GPU worker.'''

    for i in range(0, len(batch), config.EMBEDDING_BATCH_SIZE):
        yield batch[i:i + config.EMBEDDING_BATCH_SIZE]