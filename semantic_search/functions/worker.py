'''Functions for data cleaning and chunking. Meant to be
run by multiprocessing pool workers'''

# Standard imports
import multiprocessing as mp

# PyPI imports
import torch
from tokenizers import Tokenizer
from transformers import AutoTokenizer, AutoModel
from semantic_text_splitter import TextSplitter

# Internal imports
import semantic_search.configuration as config

#######################################################################
# Semantic splitting and text clean-up functions ######################
#######################################################################

def parse_text(reader_queue: mp.Queue, writer_queue: mp.Queue, worker) -> None:
    '''Takes records from reader queue. Cleans and semantically 
    splits text, sends parsed texts to writer process for batching
    and output to hdf5.'''

    print(f'Worker {worker} starting text parsing.')

    # Fire up the semantic chunk splitter
    tokenizer=Tokenizer.from_pretrained(config.TOKENIZER_NAME)
    splitter=TextSplitter.from_huggingface_tokenizer(tokenizer, config.MAX_TOKENS)

    # Main loop
    while True:
        
        # Get the next record from the reader queue.
        record=reader_queue.get()

        # Break out of the main loop when the reader process tells us we are done.
        if record == 'done':
            break

        # Strings come out of hdf5 as bytes
        text=record.decode('utf-8')

        # Do some string replacements
        text=fix_bad_symbols(text)

        # Clean up newlines
        text=clean_newlines(text)

        # Split the text into chunks
        chunks=splitter.chunks(text)

        # Send each chunk to the writer process
        for chunk in chunks:
            writer_queue.put(chunk)

    # When we break out of the main loop due to a 'done' signal from the 
    # reader process, send the same done signal along to the writer
    # process.
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


#######################################################################
# Text embedding functions ############################################
#######################################################################

def embed_text(reader_queue: mp.Queue, writer_queue: mp.Queue, gpu: str) -> None:
    '''Takes batch of text from reader queue. Calculates embeddings
    and sends batch to writer process.'''

    print(f'Worker {gpu} starting text embedding.')

    # Load the model and tokenizer
    tokenizer=AutoTokenizer.from_pretrained(config.EMBEDDING_MODEL)
    model=AutoModel.from_pretrained(config.EMBEDDING_MODEL, device_map=gpu)

    # Main loop
    while True:
        
        # Get the next record from the reader queue.
        record=reader_queue.get()

        # Break out of the main loop when the reader process tells us we are done.
        if record == 'done':
            break

        # Strings come out of hdf5 as bytes
        text=record.decode('utf-8')

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
        embedding=model_output.last_hidden_state[:,0]

        # Collect the result: the embeddings are returned as a list, even
        # don't submit a list. We also need to convert them from numpy
        # to a python list for downstream processing
        embedding=embedding[0].tolist()

        # Send the embedding to the writer process
        writer_queue.put(embedding)

    # When we break out of the main loop due to a 'done' signal from the 
    # reader process, send the same done signal along to the writer
    # process.
    writer_queue.put('done')

    # Finished
    return


#######################################################################
# OpenSearch indexing functions #######################################
#######################################################################

def make_requests(
        reader_queue: mp.Queue,
        writer_queue: mp.Queue,
        worker
) -> None:
    
    '''Takes embedded records from the reader process, formats them
    as OpenSearch indexing requests and sends them to the writer
    process for upload.'''

    print(f'Worker {worker} building OpenSearch indexing requests.')

    # Record counter, used as ID for OpenSearch bulk indexing request.
    record_count=0

    # Main loop
    while True:
        
        # Get the next record from the reader queue.
        record=reader_queue.get()

        # Break out of the main loop when the reader process tells us we are done.
        # Protect against testing string against numpy array.
        if isinstance(record, str):
            if record == 'done':
                break

        # List-likes come out of hdf5 as np.ndarray, format to vanilla python list.
        record=record.tolist()

        # Make the request.
        request=[]

        request_header={'create': {'_id': record_count}}
        request.append(request_header)

        request_body={'text_embedding': record}
        request.append(request_body)

        # Send the request to the writer process.
        writer_queue.put(request)

        # Update the record count
        record_count+=1

    # When we break out of the main loop due to a 'done' signal from the 
    # reader process, send the same done signal along to the writer
    # process.
    writer_queue.put('done')

    # Finished
    return