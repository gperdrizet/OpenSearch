'''IO functions shared by more than one task in the pipeline.'''

# Standard imports
import pathlib
import multiprocessing as mp

# PyPI imports
import h5py
from opensearchpy import OpenSearch


def hdf5_reader(
        input_file_path: str,
        reader_queue: mp.Queue,
        n_workers: int,
        task_summary: dict
) -> None:
    
    '''Reads batches from input hdf5 file and sends records to the workers via 
    the reader queue. Also collects some run statistics via the shared 
    memory task summary dictionary.'''

    # Open the input file connection
    input_data=h5py.File(input_file_path, 'r')

    # Counter for input batches and records
    batch_count=0
    record_count=0

    # Loop on the batches
    for batch_num in input_data['batches']:

        # Grab the batch from the hdf5 connection
        batch=list(input_data[f'batches/{batch_num}'])
        batch_count+=1

        # Send the records to the workers
        for record in batch:
            reader_queue.put(record)
            record_count+=1

    # Once we have sent all of the batches, send each worker a 'done' signal
    for _ in range(n_workers):
        reader_queue.put('done')

    # Add the input batch count to the parse summary
    task_summary['Input batches read']=batch_count
    task_summary['Input records read']=record_count

    # Finished
    return


def hdf5_writer(
        output_file_path: str,
        output_batch_size: int,
        writer_queue: mp.Queue,
        n_workers: int,
        task_summary: dict
) -> None:
    
    '''Collects output from workers into batches. Writes 
    them to disk as hdf5. Also collects some run statistics
    via the shared memory task summary dictionary.'''

    # Prepare the hdf5 output.
    pathlib.Path(output_file_path).unlink(missing_ok=True)
    output=h5py.File(output_file_path, 'w')
    output_batch_group=output.require_group('batches')

    # Counters and collector for output batches and records.
    output_record_count=0
    output_batch_count=0
    output_batch=[]

    # Collector for 'done' signals from workers. We break the main loop
    # once we have seen 'done' from each worker.
    done_count=0

    # Main batch aggregation loop
    while True:

        # Get the next result from the writer queue.
        result=writer_queue.get()

        # Check for 'done' signal from workers.
        if result == 'done':
            done_count+=1

            # Break the main loop once we have received 'done' from each worker.
            if done_count == n_workers:
                break
        
        # If the result is not a done signal, add it to the output batch.
        elif result != 'done':
            output_batch.append(result)
            output_record_count+=1

            # If the output batch is full, write it to disk and reset for the next.
            if len(output_batch) == output_batch_size:
                output_batch_group.create_dataset(str(output_batch_count), data=output_batch)
                output_batch_count+=1
                output_batch=[]

    # Once we break out of the main loop, write one last time to flush anything
    # remaining in the output batch to disk and close the output connection.
    output_batch_group.create_dataset(str(output_batch_count), data=output_batch)
    output_batch_count+=1
    output.close()

    # Add the batch and record count to the summary for posterity.
    task_summary['Output batches written']=output_batch_count
    task_summary['Output records written']=output_record_count

    # Finished
    return


def indexer(
        target_index_name: str,
        output_batch_size: int,
        writer_queue: mp.Queue,
        task_summary: dict

) -> None:
    '''Takes formatted requests from worker process and collects
    them into batches for indexing into OpenSearch'''

    # Create the OpenSearch index.
    initialize_index(target_index_name)

    # Initialize the OpenSearch client.
    client=start_client()

    # Counters and collector for output batches and records.
    output_record_count=0
    output_batch_count=0
    output_batch=[]

    # Main batch aggregation loop.
    while True:

        # Get the next request from the writer queue.
        request=writer_queue.get()

        # Check for 'done' signal from workers.
        if isinstance(request, str):
            if 'done' in request:
                break

        # If the request is not a done signal, add it to the output batch.
        else:

            # Add the target index name to the header adding this here
            # so that we don't have to pass this task specific parameter
            # the worker function that builds the request
            request[0]['create']['_index']=target_index_name

            # Add the request to the batch
            output_batch.extend(request)
            output_record_count+=1

            # If the output batch is full, upload it and reset for the next.
            # Need to divide length of output batch by two here because each
            # record is represented by two elements: the header and the body.
            if len(output_batch) // 2 == output_batch_size:
                _=client.bulk(output_batch)
                output_batch_count+=1
                output_batch=[]

    # Once we break out of the main loop, write one last time to flush anything
    # remaining in the output batch to disk and close the output connection.
    _=client.bulk(output_batch)
    output_batch_count+=1

    # Add the batch and record count to the summary for posterity.
    task_summary['Output batches written']=output_batch_count
    task_summary['Output records written']=output_record_count

    # Finished
    return

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


def initialize_index(index_name: str) -> None:
    '''Set-up OpenSearch index. Deletes index if it already exists
    at run start. Creates new index for run.'''

    client=start_client()

    # Delete the index we are trying to create if it exists.
    if client.indices.exists(index=index_name):
        _=client.indices.delete(index=index_name)

    # Create the target index if it does not exist.
    if client.indices.exists(index=index_name) is False:

        index_body={
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

        _=client.indices.create(index_name, body=index_body)

    # Close client
    client.close()
