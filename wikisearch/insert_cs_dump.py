import json
from gzip import GzipFile

import wikisearch.functions.IO_functions as io_funcs

################################################################################

def run(
    input_file: str,
    index_name: str,
) -> None:
    
    '''Takes a bz2 format CirrusSearch index dump and 
    bulk inserts it into OpenSearch'''

    print()

    # Start the OpenSearch client and create the index
    client=io_funcs.start_client(index_name)

    # Open the input file with gzip
    wiki=GzipFile(input_file)

    # Collect and insert batches

    # Holder for batch
    batch=[]

    # Counter for batches
    batch_count=0

    # counter for article ids
    id_num=0

    # Loop on lines
    for line in wiki:

        # Convert line to dict
        line=json.loads(line)

        # Need to make some changes to the index dicts to make them
        # compatible

        if 'index' in line.keys():

            # Remove unsupported '_type'
            line['index'].pop('_type', None)

            # Add index name
            line['index']['_index']=index_name

            # replace the id with a sequential number
            line['index']['_id']=id_num

            # Increment the id num for next time
            id_num+=1

        # Add to batch
        batch.append(line)

        # Once we have 1000 lines, bulk insert them
        if len(batch) == 1000:
            _=client.bulk(batch)

            # Clear the batch to collect the next
            batch = []

            # Count it
            batch_count+=1
            print(f'Batch {batch_count} inserted', end='\r')
