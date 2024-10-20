'''Data pipeline functions, meant to be called by Luigi tasks.'''

# Standard imports
import time
import json
import pathlib
import multiprocessing as mp
import multiprocessing as mp
from multiprocessing import Manager, Process

# PyPI imports
import h5py
from opensearchpy import exceptions

# Internal imports
import semantic_search.configuration as config
import semantic_search.functions.common_io as io_funcs
import semantic_search.functions.embedding as embed_funcs
import semantic_search.functions.parsing as parse_funcs
import semantic_search.functions.opensearch_loader as loader_funcs
from semantic_search.functions.wikipedia_extractor import wikipedia_extractor


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

    # Start multiprocessing manager
    manager=Manager()

    # Set-up the summary as a shared variable via the multiprocessing manager
    # so that both the reader and writer processes can add some summary
    # statistics to it when they finish.
    task_summary=manager.dict(source_config)

    # Set-up reader and writer queues to move records from the reader
    # process to the workers and from the workers to the writer process.
    reader_queue=manager.Queue(maxsize=10000)
    writer_queue=manager.Queue(maxsize=10000)

    # Set-up reader and writer processes: reader gets records from the input
    # file and sends them to the workers, writer takes results from workers
    # collects them into batches and writes to output file.

    # Set worker count based on avalible CPUs. Subtract three: one
    # for the reader and writer processes and one for the system.
    n_workers=mp.cpu_count() - 3

    # IO paths
    input_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.EXTRACTED_TEXT}"
    output_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.PARSED_TEXT}"

    reader_process=Process(
        target=io_funcs.hdf5_reader,
        args=(
            input_file_path, 
            reader_queue, 
            n_workers,
            task_summary
        )
    )

    writer_process=Process(
        target=io_funcs.hdf5_writer,
        args=(
            output_file_path,
            source_config['output_batch_size'],
            writer_queue,
            n_workers,
            task_summary
        )
    )

    # Start the worker pool
    pool=mp.Pool(processes=n_workers)

    # Start each worker
    for _ in range(n_workers):
       pool.apply_async(parse_funcs.parse_text, (reader_queue,writer_queue,))

    # Start the reader and writer processes to begin real work, timing how long it takes.
    start_time=time.time()

    reader_process.start()
    writer_process.start()

    # Wait for the pool workers to finish, then shut the pool down.
    pool.close()
    pool.join()

    # Stop the timer
    dT=time.time() - start_time

    # Clean up IO processes
    reader_process.join()
    reader_process.close()

    writer_process.join()
    writer_process.close()

    # Write some stuff to the summary then recover it to a normal python dictionary
    # from the multiprocessing shared memory DictProxy object
    task_summary['Processing rate (records per second)']=task_summary['Input records read']/dT
    run_time=config.WIKIPEDIA_RECORD_COUNT / task_summary['Processing rate (records per second)']
    task_summary['Estimated total run time (seconds)']=run_time
    task_summary=dict(task_summary)

    # Close the queues and stop the manager
    manager.shutdown()

    # Finished
    return task_summary


def embed_data(data_source: str) -> dict:
    '''Uses HuggingFace transformers to pre-calculate embeddings for indexing.'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Start multiprocessing manager
    manager=Manager()

    # Set-up the task summary as a shared variable via the multiprocessing
    # manager so that both the reader and writer processes can add some summary
    # statistics to it when they finish.
    task_summary=manager.dict(source_config)

    # Set-up reader and writer queues to move records from the reader
    # process to the workers and from the workers to the writer process.
    reader_queue=manager.Queue(maxsize=10000)
    writer_queue=manager.Queue(maxsize=10000)

    # Set-up reader and writer processes: reader gets records from the input
    # file and sends them to the workers, writer takes results from workers
    # collects them into batches and writes to output file.

    # Set embedding worker count based on GPUs listed in the configuration file
    n_workers=len(config.WORKER_GPUS)

    # IO paths
    input_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.PARSED_TEXT}"
    output_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.EMBEDDED_TEXT}"

    reader_process=Process(
        target=io_funcs.hdf5_reader,
        args=(
            input_file_path, 
            reader_queue, 
            n_workers,
            task_summary
        )
    )

    writer_process=Process(
        target=io_funcs.hdf5_writer,
        args=(
            output_file_path,
            source_config['output_batch_size'],
            writer_queue,
            n_workers,
            task_summary
        )
    )

    # Start the embedder pool
    embedder_pool=mp.Pool(processes=n_workers)

    # Start each parse worker
    for gpu in config.WORKER_GPUS:
       embedder_pool.apply_async(embed_funcs.embed_text, (reader_queue,writer_queue,gpu,))

    # Start the reader and writer processes to begin real work, timing how long it takes.
    start_time=time.time()

    reader_process.start()
    writer_process.start()

    # Wait for the embedder pool workers to finish, then shut the pool down.
    embedder_pool.close()
    embedder_pool.join()

    # Stop the timer
    dT=time.time() - start_time

    # Clean up IO processes
    reader_process.join()
    reader_process.close()

    writer_process.join()
    writer_process.close()

    # Write some stuff to the summary then recover it to a normal python dictionary
    # from the multiprocessing shared memory DictProxy object
    task_summary['Processing rate (records per second)']=task_summary['Input records read']/dT
    run_time=config.WIKIPEDIA_RECORD_COUNT / task_summary['Processing rate (records per second)']
    task_summary['Estimated total run time (seconds)']=run_time
    task_summary=dict(task_summary)

    # Close the queues and stop the manager
    manager.shutdown()

    # Finished
    return task_summary


def load_data(data_source: str) -> dict:
    '''Loads embedded data into OpenSearch KNN vector database for semantic search.'''

        # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Start multiprocessing manager
    manager=Manager()

    # Set-up the task summary as a shared variable via the multiprocessing
    # manager so that both the reader and writer processes can add some summary
    # statistics to it when they finish.
    task_summary=manager.dict(source_config)

    # Set-up reader and writer queues to move records from the reader
    # process to the workers and from the workers to the writer process.
    reader_queue=manager.Queue(maxsize=10000)
    writer_queue=manager.Queue(maxsize=10000)

    # Set-up reader and writer processes: reader gets records from the input
    # file and sends them to the workers, writer takes results from workers
    # collects them into batches and writes to output file.

    # Only use one worker for bulk indexing
    n_workers=1

    # IO paths
    input_file_path=input_file_path=f"{config.DATA_PATH}/{source_config['target_index_name']}/{config.EMBEDDED_TEXT}"

    reader_process=Process(
        target=io_funcs.hdf5_reader,
        args=(
            input_file_path, 
            reader_queue, 
            n_workers,
            task_summary
        )
    )

    writer_process=Process(
        target=loader_funcs.indexer,
        args=(
            source_config['target_index_name'],
            config.BULK_INSERT_BATCH_SIZE,
            writer_queue,
            #n_workers,
            task_summary
        )
    )

    # Start the embedder pool
    embedder_pool=mp.Pool(processes=n_workers)

    # Start each parse worker
    for _ in range(n_workers):
       embedder_pool.apply_async(loader_funcs.make_requests, (reader_queue,writer_queue,source_config['target_index_name'],))

    # Start the reader and writer processes to begin real work, timing how long it takes.
    start_time=time.time()

    reader_process.start()
    writer_process.start()

    # Wait for the embedder pool workers to finish, then shut the pool down.
    embedder_pool.close()
    embedder_pool.join()

    # Stop the timer
    dT=time.time() - start_time

    # Clean up IO processes
    reader_process.join()
    reader_process.close()

    writer_process.join()
    writer_process.close()

    # Write some stuff to the summary then recover it to a normal python dictionary
    # from the multiprocessing shared memory DictProxy object
    task_summary['Processing rate (records per second)']=task_summary['Input records read']/dT
    run_time=config.WIKIPEDIA_RECORD_COUNT / task_summary['Processing rate (records per second)']
    task_summary['Estimated total run time (seconds)']=run_time
    task_summary=dict(task_summary)

    # Close the queues and stop the manager
    manager.shutdown()

    # Finished
    return task_summary

    # # Load the data source configuration
    # source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    # with open(source_config_path, encoding='UTF-8') as source_config_file:
    #     source_config=json.load(source_config_file)

    # # Start the load summary with the data from the source configuration
    # load_summary=source_config

    # # Open the input
    # input_file_path=(f"{config.DATA_PATH}/{source_config['target_index_name']}" +
    #     f'/{config.EMBEDDED_TEXT}')

    # input_data=h5py.File(input_file_path, 'r')

    # # Create the OpenSearch index
    # loader_funcs.initialize_index(source_config['target_index_name'])

    # # Initialize the OpenSearch client
    # client=loader_funcs.start_client()

    # # Count records and batches
    # record_count=0
    # batch_count=0

    # # Holder to collect batch for bulk insert
    # bulk_insert_batch=[]

    # # Start the timer
    # start_time = time.time()

    # # Loop on the input batches
    # for batch_num in input_data['batches']:

    #     # Grab the batch from the hdf5 connection
    #     input_batch=input_data[f'batches/{batch_num}']

    #     # Loop on the embedded texts in the input batch, collecting them for indexing
    #     for embeddings in input_batch:
    #         bulk_insert_batch.append(embeddings)

    #         # If the batch is full, insert it
    #         if len(bulk_insert_batch) == config.BULK_INSERT_BATCH_SIZE:

    #             # Insert, Catching any connection timeout errors from OpenSearch
    #             try:

    #                 record_count=loader_funcs.index_batch(client, bulk_insert_batch, source_config, record_count)
    #                 batch_count+=1
    #                 bulk_insert_batch=[]

    #             # If we catch a connection timeout or transport error, sleep for a bit and
    #             # don't clear the batch before continuing the loop
    #             except (exceptions.ConnectionTimeout, exceptions.TransportError):
    #                 time.sleep(10)

    # # If we finish consuming the input and have embeddings that did not get indexed
    # # because we did not have enough to fill the last bulk indexing batch, index them
    # if len(bulk_insert_batch) > 0:
    #     record_count=loader_funcs.index_batch(client, bulk_insert_batch, source_config, record_count)

    # dT=time.time() - start_time # pylint: disable = invalid-name

    # # Add some stuff the the summary
    # load_summary['run_time_seconds']=dT
    # load_summary['indexed_batches']=batch_count
    # load_summary['indexing_batch_size']=config.BULK_INSERT_BATCH_SIZE
    # load_summary['indexed_records']=record_count
    # load_summary['observed_indexing_rate']=(record_count/dT)
    # load_summary['estimated_total_indexing_time']=(config.WIKIPEDIA_ESTIMATED_CHUNK_COUNT / load_summary['observed_indexing_rate'])

    # # Close the hdf5 connection
    # input_data.close()

    # return load_summary
