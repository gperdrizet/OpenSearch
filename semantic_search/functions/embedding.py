'''Functions to embed text for indexing into KNN index.'''

# Standard imports
import multiprocessing as mp

# PyPI imports
import torch
from transformers import AutoTokenizer, AutoModel

# Internal imports
import semantic_search.configuration as config


def embed_text(reader_queue: mp.Queue, writer_queue: mp.Queue, gpu: str) -> None:
    '''Takes batch of text from reader queue. Calculates embeddings
    and sends batch to writer process.'''

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