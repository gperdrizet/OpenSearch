'''Functions to embed text for indexing into KNN index.'''

# Standard imports
import multiprocessing as mp

# PyPI imports
import h5py
import torch
from transformers import AutoTokenizer, AutoModel

# Internal imports
import semantic_search.configuration as config

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