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
    input_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.BATCHED_TEXT}"
    input_data=h5py.File(input_file_path, 'r')

    # Set number of workers to one less than the CPU count and create the pool
    n_workers=mp.cpu_count() - 1
    pool=mp.Pool(processes=n_workers)

    # Counters and accumulators for batch loop
    batch_count=1
    record_count=1
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

        # Once we have a batch for each worker, run this round
        if len(batches) == n_workers:

            # Holder for results from workers
            worker_results=[]

            # Submit each batch to a worker
            for batch in batches:
                worker_result=pool.apply_async(parse_funcs.clean_and_chunk, (batch,))
                worker_results.append(worker_result)

            # Collect the results from the workers
            results=[worker_result.get() for worker_result in worker_results]

            # Save each result as a batch in the hdf5 file
            for result in results:
                output_batch_group.create_dataset(str(batch_count), data=result)
                batch_count+=1

            # Reset batches for next round
            batches=[]

    dT=time.time() - start_time # pylint: disable = invalid-name

    # Add some stuff the the summary
    transform_summary['run_time_seconds']=dT
    transform_summary['worker_processes']=n_workers
    transform_summary['parsed_batches']=batch_count
    transform_summary['parsed_records']=record_count
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
    pool=mp.Pool(processes=n_workers, maxtasksperchild=1)

    # Counters and accumulators for batch loop
    batch_count=0
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

            # If the batch is full, break the line loop
            if len(decoded_batch) == config.EMBEDDING_BATCH_SIZE * config.WORKER_BATCHES_PER_ROUND:
                break

        # If we left the batch loop because the batch is full...
        if len(decoded_batch) == config.EMBEDDING_BATCH_SIZE * config.WORKER_BATCHES_PER_ROUND:

            # Add the decoded batch to this round
            batches.append(decoded_batch)

            # Once we have a batch for each worker, run this round
            if len(batches) == n_workers:

                print(f'Have {len(batches)} batches for {n_workers} GPUs')

                # Holder for results from workers
                worker_results=[]

                # Submit each batch to a worker
                for batch, gpu in zip(batches, config.WORKER_GPUS):
                    worker_result=pool.apply_async(embed_funcs.calculate_embeddings, (batch,gpu,))
                    worker_results.append(worker_result)

                # Collect the results from the workers
                results=[worker_result.get() for worker_result in worker_results]

                # Save each result as a batch in the hdf5 file
                for result in results:
                    output_batch_group.create_dataset(str(batch_count), data=result)
                    batch_count+=1

                # Reset batches for next round
                batches=[]

    dT=time.time() - start_time # pylint: disable = invalid-name

    # Add some stuff the the summary
    embedding_summary['run_time_seconds']=dT
    embedding_summary['worker_processes']=n_workers
    embedding_summary['embedding_batches_per_worker']=config.WORKER_BATCHES_PER_ROUND
    embedding_summary['embedded_batches']=batch_count * config.WORKER_BATCHES_PER_ROUND
    embedding_summary['embedding_batch_size']=config.EMBEDDING_BATCH_SIZE
    embedding_summary['embedded_records']=record_count
    embedding_summary['observed_embedding_rate']=(record_count/dT)
    embedding_summary['estimated_total_embedding_time']=(config.WIKIPEDIA_RECORD_COUNT / embedding_summary['observed_embedding_rate'])

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

        # Loop on the text in the batch, collecting them for indexing
        for embeddings in batch:
            bulk_insert_batch.append(embeddings)

            # If the batch is full, break the line loop
            if len(bulk_insert_batch) == config.BULK_INSERT_BATCH_SIZE:
                break

        # If we left the batch loop because the batch is full...
        if len(bulk_insert_batch) == config.BULK_INSERT_BATCH_SIZE:
        
            # Build the requests
            knn_requests=[]

            for embedded_text in bulk_insert_batch:

                record_count+=1

                knn_request_header={
                    'index': {
                        '_index': source_config['target_index_name'],
                        '_id': record_count
                    }
                }

                knn_requests.append(knn_request_header)

                request_body={'text_embedding': embedded_text}

                knn_requests.append(request_body)

            # Insert, Catching any connection timeout errors from OpenSearch
            try:

                # Do the insert
                _=client.bulk(knn_requests)

                # Clear the batch
                bulk_insert_batch=[]
                batch_count+=1

            # If we catch an connection timeout or transport error, sleep for a bit and
            # don't clear the batch before continuing the loop
            except (exceptions.ConnectionTimeout, exceptions.TransportError):
                time.sleep(10)

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
