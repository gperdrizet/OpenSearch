'''Collection of functions for notebooks.'''

# Standard imports
import random
import time
import multiprocessing as mp

# PyPI imports
import h5py
import torch
from transformers import AutoTokenizer, AutoModel
from opensearchpy import OpenSearch # pylint: disable = import-error

# Internal imports
import configuration as config

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

# Index definitions
TEXT_INDEX_BODY={
    'settings': {
        'index': {
            'number_of_shards': 3
        }
    },
    "mappings": {
        "properties": {
            "text": {
                "type": "text"
            }
        }
    }
}

KNN_INDEX_BODY={
    "settings": {
        "number_of_shards": 3,
        "index.knn": "true",
        "default_pipeline": "embedding-ingest-pipeline"
    },
    "mappings": {
        "properties": {
            "text_embedding": {
                "type": "knn_vector",
                "dimension": 768,
                "method": {
                    "engine": "lucene",
                    "space_type": "l2",
                    "name": "hnsw",
                    "parameters": {}
                }
            },
            "text": {
                "type": "text"
            }
        }
    }
}

PRE_EMBEDDED_KNN_INDEX={
  "settings": {
    "index": {
      "number_of_shards": 3,
      "knn": "true",
      "knn.algo_param.ef_search": 100
    }
  },
  "mappings": {
    "properties": {
      "text_embedding": {
        "type": "knn_vector",
        "dimension": 768,
        "space_type": "l2",
        "method": {
          "name": "hnsw",
          "engine": "lucene",
          "parameters": {
            "ef_construction": 128,
            "m": 24
          }
        }
      }
    }
  }
}


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
    worker_gpus: list,
    batches: list
):

    '''Takes list of batches and list of worker GPUs, submits batches 
    to worker pool for embedding.'''

    # Holder for results from workers
    worker_embedding_times=[]

    # Start the pool
    pool=mp.Pool(processes=len(worker_gpus))

    # Submit each batch to a worker
    for batch, gpu in zip(batches, worker_gpus):
        worker_embedding_time=pool.apply_async(calculate_embeddings, (batch,gpu,))
        worker_embedding_times.append(worker_embedding_time)

    # Collect the results from the workers
    worker_embedding_times=[worker_embedding_time.get() for worker_embedding_time in worker_embedding_times]

    # Get the mean embedding time
    mean_embedding_time=sum(worker_embedding_times) / len(worker_embedding_times)

    return mean_embedding_time


def calculate_embeddings(batch: list, gpu: str) -> float:
    '''Takes batch of text and gpu identifier, embeds with embedding batch
    size of one, returns total embedding time.'''

    embedding_model='sentence-transformers/msmarco-distilbert-base-tas-b'

    # Load the model and tokenizer
    tokenizer=AutoTokenizer.from_pretrained(embedding_model)
    model=AutoModel.from_pretrained(embedding_model, device_map=gpu)

    # Holder for embedded texts
    embedded_texts=[]

    # Start the timer
    start_time=time.time()

    # Loop texts in batch
    for text in batch:

        # Tokenize the texts
        encoded_input=tokenizer(
            text.decode('utf-8'),
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
        embedded_texts.extend(embeddings.tolist())

    # Get embedding time
    dT=time.time() - start_time

    # Return the embedding time
    return dT


def reader(
        records: list,
        target_texts: int,
        batch_size: int,
        reader_queue: mp.Queue, # type: ignore,
        n_workers
) -> None:
    
    '''Yields randomly sampled batches of input text and puts them
    into the reader queue until the target number of texts has been
    reached, then sends stop signals.'''

    text_count=0

    while text_count < target_texts:

        # Grab a random sample of texts
        batch=random.sample(records, batch_size)

        # Decode each record in the batch
        texts=[record.decode('utf-8') for record in batch]

        # Put the batch in the queue
        reader_queue.put(texts)

        # Update the number of texts submitted
        text_count+=len(texts)

    # Once we have put all of the text needed into the queue
    # send a done signal for each worker and finish
    for _ in range(n_workers):
        reader_queue.put('Done')

    return


def calculate_embeddings_from_queue(
        gpu: str,
        reader_queue: mp.Queue
) -> None:
    
    '''Takes text from queue and calculates embeddings until
    Done string is received.'''

    embedding_model='sentence-transformers/msmarco-distilbert-base-tas-b'

    # Load the model and tokenizer
    tokenizer=AutoTokenizer.from_pretrained(embedding_model)
    model=AutoModel.from_pretrained(embedding_model, device_map=gpu)

    # Holder for embedded texts
    embedded_texts=[]

    # Loop until we receive done from the reader
    while True:

        # Get a batch of text from the queue
        batch=reader_queue.get()

        if batch == 'Done':
            return

        else:

            # Loop on the batch
            for text in batch:

                # Tokenize the text
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
                embedded_texts.extend(embeddings.tolist())