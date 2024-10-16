'''Functions to embed text for indexing into KNN index.'''

# PyPI imports
from transformers import AutoTokenizer, AutoModel
import torch

# Internal imports
import semantic_search.configuration as config

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