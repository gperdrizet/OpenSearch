'''Collection of functions for loading data into OpenSearch.'''

# Standard imports
import multiprocessing as mp

# PyPI imports
from opensearchpy import OpenSearch

# Internal imports
import semantic_search.configuration as config

def make_requests(
        reader_queue: mp.Queue,
        writer_queue: mp.Queue,
        target_index_name: str
) -> None:
    
    '''Takes embedded records from the reader process, formats them
    as OpenSearch indexing requests and sends them to the writer
    process for upload.'''

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

        request_header={'create': {'_index': target_index_name,'_id': record_count}}
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


def indexer(
        target_index_name: str,
        output_batch_size: int,
        writer_queue: mp.Queue,
        #n_workers: int,
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

        # Get the next result from the writer queue.
        result=writer_queue.get()

        # Check for 'done' signal from workers.
        if isinstance(result, str):
            if 'done' in result:
                break

        # If the result is not a done signal, add it to the output batch.
        else:
            output_batch.extend(result)
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

