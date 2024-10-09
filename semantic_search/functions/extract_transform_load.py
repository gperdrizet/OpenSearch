'''Data pipeline functions, meant to be called by Luigi tasks.'''

# Standard imports
import time
import json
from gzip import GzipFile

# PyPI imports
import h5py

# Internal imports
import semantic_search.configuration as config

def extract_data(data_source: str) -> dict:
    '''Wrapper function to call correct task specific data
    extractor function'''

    # Load the data source configuration
    source_config_path=f'{config.DATA_SOURCE_CONFIG_PATH}/{data_source}.json'

    with open(source_config_path, encoding='UTF-8') as source_config_file:
        source_config=json.load(source_config_file)

    # Pick the extractor function to run based on the data source configuration
    extractor_function=globals()[source_config['extractor_function']]

    # Run the extraction
    extraction_summary=extractor_function(source_config)

    return extraction_summary

def wikipedia_extractor(source_config: dict) -> dict:
    '''Runs text extraction and batching on CirrusSearch Wikipedia dump.'''

    # Start the extraction summary with the data from the source configuration
    extraction_summary = source_config

    # Prepare the hdf5 output
    output_file=f"{config.DATA_PATH}/{source_config['output_data_dir']}/{config.BATCHED_TEXT}"
    output=h5py.File(output_file, 'w')
    batch_group=output.require_group('batches')

    # Open the input file stream
    gzip_data_file_path=f"{config.RAW_DATA_PATH}/{source_config['raw_data_file']}"
    file=GzipFile(gzip_data_file_path)

    # Loop on the line from the input file stream and accumulate batches
    line_count=0
    batch_count=0
    batch=[]

    start_time = time.time()

    for line in file:

        line_count+=1

        # Only pull text from article records (every other record is a metadata header)
        if line_count % 2 == 0:
            record=json.loads(line)
            text=record['text']
            batch.append(text)

        # Once the batch is full, save it and reset
        if len(batch) == source_config['batch_size']:
            batch_group.create_dataset(str(batch_count), data=batch)
            batch_count+=1
            batch=[]

    # Once we have looped trough the file, save any remaining records
    # as one last batch
    if len(batch) != 0:
        batch_group[str(batch_count)]=batch

    dT=time.time() - start_time

    # Add some stuff the the summary
    extraction_summary['num_batches']=batch_count
    extraction_summary['run_time_seconds']=dT

    # Add some metadata to the hdf5 file
    metadata={'data_source': 'wikipedia','num_batches': batch_count}
    output.attrs.update(metadata)

    # Close the hdf5 file
    output.close()

    return extraction_summary
