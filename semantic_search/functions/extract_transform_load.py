'''Data pipeline functions, meant to be called by Luigi tasks.'''

# Standard imports
import time
import json
import pathlib
import multiprocessing as mp

# PyPI imports
import h5py

# Internal imports
import semantic_search.configuration as config
import semantic_search.functions.data_transformation as transform_funcs
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


def transform_data(data_source: str) -> dict:
    '''Runs data normalization and chunking on pre-batched data.'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Start the transform summary with the data from the source configuration
    transform_summary=source_config

    # Prepare the hdf5 output
    output_file=f"{config.DATA_PATH}/{source_config['output_data_dir']}/{config.TRANSFORMED_TEXT}"
    pathlib.Path(output_file).unlink(missing_ok=True)
    output=h5py.File(output_file, 'w')
    output_batch_group=output.require_group('batches')

    # Open the input
    input_file_path=f"{config.DATA_PATH}/{source_config['output_data_dir']}/{config.BATCHED_TEXT}"
    input_data=h5py.File(input_file_path, 'r')

    # Set number of workers to one less than the CPU count and create the pool
    n_workers=mp.cpu_count() - 1
    pool=mp.Pool(processes=n_workers)

    # Counters and accumulators for batch loop
    batch_count=1
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
            decoded_batch.append(text.decode('utf-8'))

        # Add the decoded batch to this round
        batches.append(decoded_batch)

        # Once we have a batch for each worker, run this round
        if len(batches) == n_workers:

            # Holder for results from workers
            worker_results=[]

            # Submit each batch to a worker
            for batch in batches:
                worker_result=pool.apply_async(transform_funcs.clean_and_chunk, (batch,))
                worker_results.append(worker_result)

            # Collect the results from the workers
            results=[worker_result.get() for worker_result in worker_results]

            # Save each result as a batch in the hdf5 file
            for result in results:
                output_batch_group.create_dataset(str(batch_count), data=result)
                batch_count+=1

            # Reset batches for next round
            batches=[]

    dT=start_time - time.time() # pylint: disable = invalid-name

    # Add some stuff the the summary
    transform_summary['run_time_seconds']=dT

    # Close the hdf5 file
    output.close()

    return transform_summary
