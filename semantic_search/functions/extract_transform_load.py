'''Data pipeline functions, meant to be called by Luigi tasks.'''

# Standard imports
import time
import json
import pathlib
import multiprocessing as mp

# PyPI imports
import h5py
from opensearchpy import exceptions # pylint: disable = import-error

# Internal imports
import semantic_search.configuration as config
import semantic_search.functions.embedding as embed_funcs
import semantic_search.functions.parsing as parse_funcs
import semantic_search.functions.opensearch_loader as loader_funcs
from semantic_search.functions.wikipedia_extractor import wikipedia_extractor # pylint: disable = unused-import


def extract_data(data_source: str) -> dict:
    '''Wrapper function to call correct task specific data extractor function.'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Pick the extractor function to run based on the data source configuration
    extractor_function=globals()[source_config['extractor_function']]

    # Run the extraction
    extraction_summary=extractor_function(source_config)

    return extraction_summary


def parse_data(data_source: str) -> dict:
    '''Runs data normalization and chunking on pre-batched data.'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Start the transform summary with the data from the source configuration
    transform_summary=source_config

    # Prepare the hdf5 output
    output_file=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.PARSED_TEXT}"
    pathlib.Path(output_file).unlink(missing_ok=True)
    output=h5py.File(output_file, 'w')
    output_batch_group=output.require_group('batches')

    # Open the input
    input_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.EXTRACTED_TEXT}"
    input_data=h5py.File(input_file_path, 'r')

    # Set number of workers to one less than the CPU count
    n_workers=mp.cpu_count() - 1

    # Counters and accumulators for batch loop
    batch_count=0
    chunk_count=0
    record_count=0
    batches=[]

    # Start the timer
    start_time = time.time()

    # Loop on the batches
    for batch_num in input_data['batches']:

        # Grab the batch from the hdf5 connection
        batch=input_data[f'batches/{batch_num}']

        # Strings come out of hdf5 as bytes, decode them
        decoded_batch=[]

        for text in batch:
            record_count+=1
            decoded_batch.append(text.decode('utf-8'))

        # Add the decoded batch to this round
        batches.append(decoded_batch)

        # There are three ways we to enter job submission:
        #
        # 1. We have a batch for every worker.
        # 2. The number of batches left to submit in order to complete the number of
        #    batches requested by the user is less than the number of workers.
        # 3. We have reached the end of the input.
        #
        # Each of these three cases also has a different thing to do after. For number
        # we submit the jobs and keep going. For number two, we break the line loop and stop.
        # for number three, we have already left the line loop and can just end after
        # the last batches.

        # 1. If we have a batch for each worker, submit
        if len(batches) == n_workers:

            batch_count, chunk_count=parse_funcs.submit_batches(n_workers, batches, output_batch_group, batch_count, chunk_count)

            # Reset batches for next round
            batches=[]

        # 2. If the number of batches left to fulfill a user request for a specific number of
        # batches is less than the number of workers, submit that number of batches
        # once we have them.
        if source_config['num_batches'] != 'all':

            batches_remaining=source_config['num_batches'] - batch_count

            if batches_remaining < n_workers and len(batches) == batches_remaining:

                n_workers=batches_remaining
                batch_count, chunk_count=parse_funcs.submit_batches(n_workers, batches, output_batch_group, batch_count, chunk_count)

                # Break the line loop to end the run
                break
            
            # Also break the line loop if we have completed the exact number of batches
            # requested by the user
            if batch_count == source_config['num_batches']:
                break

    # 3. If we are extracting all of the data, i.e. the user has not requested a specific
    # number of batches to run, submit whatever is left over after the last extraction
    # round after we have finished reading the input file
    if source_config['num_batches'] == 'all':

        # If we have batches that did not get processed because we ran out of input
        # before collecting enough batches for a full extraction round, start a
        # round with what we have
        if len(batches) != 0:

            n_workers=len(batches)
            batch_count, chunk_count=parse_funcs.submit_batches(n_workers, batches, output_batch_group, batch_count, chunk_count)

    dT=time.time() - start_time # pylint: disable = invalid-name

    # Add some stuff the the summary
    transform_summary['run_time_seconds']=dT
    transform_summary['worker_processes']=n_workers
    transform_summary['parsed_batches']=batch_count
    transform_summary['parsed_records']=record_count
    transform_summary['output_chunks']=chunk_count
    transform_summary['observed_parse_rate']=(record_count/dT)
    transform_summary['estimated_total_parse_time']=(config.WIKIPEDIA_RECORD_COUNT / transform_summary['observed_parse_rate'])

    # Close the hdf5
    input_data.close()
    output.close()

    return transform_summary


def embed_data(data_source: str) -> dict:
    '''Uses HuggingFace transformers to pre-calculate embeddings for indexing.
    Parallelizes embedding over batches'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Start the transform summary with the data from the source configuration
    embedding_summary=source_config

    # Prepare the hdf5 output
    output_file=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.EMBEDDED_TEXT}"
    pathlib.Path(output_file).unlink(missing_ok=True)
    output=h5py.File(output_file, 'w')
    output_batch_group=output.require_group('batches')

    # Open the input
    input_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.PARSED_TEXT}"
    input_data=h5py.File(input_file_path, 'r')

    # Set number of workers using the GPU list from the configuration file
    n_workers=len(config.WORKER_GPUS)

    # Counters and accumulators for batch loop
    batch_count=0
    record_count=0
    batches=[]
    decoded_batch=[]

    # Start the timer
    start_time = time.time()

    # Loop on the batches
    for batch_num in input_data['batches']:

        # Grab the batch from the hdf5 connection
        batch=input_data[f'batches/{batch_num}']

        # Strings come out of hdf5 as bytes, decode them
        for text in batch:
            record_count+=1
            decoded_batch.append(text.decode('utf-8'))

            # If the batch is full...
            if len(decoded_batch) == config.EMBEDDING_BATCH_SIZE * config.WORKER_BATCHES_PER_ROUND:

                # Add the decoded batch to this round
                batches.append(decoded_batch)
                decoded_batch=[]

                # Once we have a batch for each worker, run this round
                if len(batches) == n_workers:

                    batch_count=embed_funcs.submit_batches(n_workers, batches, output_batch_group, batch_count)

                    # Reset batches for next round
                    batches=[]

    # Once we reach the end of the input batches, make sure we submit anything left over for embedding
    if len(decoded_batch) > 0:
        batches.append(decoded_batch)

    if len(batches) > 0:
        n_workers=len(batches)
        batch_count=embed_funcs.submit_batches(n_workers, batches, output_batch_group, batch_count)


    dT=time.time() - start_time # pylint: disable = invalid-name

    # Add some stuff the the summary
    embedding_summary['run_time_seconds']=dT
    embedding_summary['worker_processes']=n_workers
    embedding_summary['worker_batches_per_worker']=config.WORKER_BATCHES_PER_ROUND
    embedding_summary['embedded_batches']=batch_count * config.WORKER_BATCHES_PER_ROUND
    embedding_summary['embedding_batch_size']=config.EMBEDDING_BATCH_SIZE
    embedding_summary['embedded_records']=record_count
    embedding_summary['observed_embedding_rate']=(record_count/dT)
    embedding_summary['estimated_total_embedding_time']=(config.WIKIPEDIA_ESTIMATED_CHUNK_COUNT / embedding_summary['observed_embedding_rate'])

    # Close the hdf5s
    input_data.close()
    output.close()
    
    return embedding_summary


def load_data(data_source: str) -> dict:
    '''Loads embedded data into OpenSearch KNN vector database for semantic search.'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Start the load summary with the data from the source configuration
    load_summary=source_config

    # Open the input
    input_file_path=(f"{config.DATA_PATH}/{source_config['target_index_name']}" +
        f'/{config.EMBEDDED_TEXT}')

    input_data=h5py.File(input_file_path, 'r')

    # Create the OpenSearch index
    loader_funcs.initialize_index(source_config['target_index_name'])

    # Initialize the OpenSearch client
    client=loader_funcs.start_client()

    # Count records and batches
    record_count=0
    batch_count=0

    # Holder to collect batch for bulk insert
    bulk_insert_batch=[]

    # Start the timer
    start_time = time.time()

    # Loop on the batches
    for batch_num in input_data['batches']:

        # Grab the batch from the hdf5 connection
        batch=input_data[f'batches/{batch_num}']
        print(f'Input batch {batch_num} has {len(batch)} embeddings')

        # Loop on the embedded texts in the input batch, collecting them for indexing
        for embeddings in batch:
            bulk_insert_batch.append(embeddings)

            # If the batch is full, insert it
            if len(bulk_insert_batch) == config.BULK_INSERT_BATCH_SIZE:
                print(f'Indexing batch of {len(bulk_insert_batch)} embeddings')

                # Insert, Catching any connection timeout errors from OpenSearch
                try:

                    record_count=loader_funcs.index_batch(client, bulk_insert_batch, source_config, record_count)
                    batch_count+=1
                    bulk_insert_batch=[]

                # If we catch a connection timeout or transport error, sleep for a bit and
                # don't clear the batch before continuing the loop
                except (exceptions.ConnectionTimeout, exceptions.TransportError):
                    time.sleep(10)

    # If we finish consuming the input and have embeddings that did not get indexed
    # because we did not have enough to fill the last bulk indexing batch, index them
    if len(bulk_insert_batch) > 0:
        record_count=loader_funcs.index_batch(client, bulk_insert_batch, source_config, record_count)

    dT=time.time() - start_time # pylint: disable = invalid-name

    # Add some stuff the the summary
    load_summary['run_time_seconds']=dT
    load_summary['indexed_batches']=batch_count
    load_summary['indexing_batch_size']=config.BULK_INSERT_BATCH_SIZE
    load_summary['indexed_records']=record_count
    load_summary['observed_indexing_rate']=(record_count/dT)
    load_summary['estimated_total_indexing_time']=(config.WIKIPEDIA_ESTIMATED_CHUNK_COUNT / load_summary['observed_indexing_rate'])

    # Close the hdf5s
    input_data.close()

    return load_summary
