import json
from gzip import GzipFile
from multiprocessing import Manager, Process
from threading import Thread

from wikisearch.classes.cirrussearch_reader import CirrusSearchReader
import wikisearch.functions.IO_functions as io_funcs
import wikisearch.functions.parsing_functions as parse_funcs

################################################################################

def run(
    input_file: str,
    index_name: str,
    output_destination: str
) -> None:
    
    '''Main function to run CirrusSearch dump parse'''

    # Start multiprocessing manager
    manager=Manager()

    # Set-up queues
    output_queue=manager.Queue(maxsize=2000)
    input_queue=manager.Queue(maxsize=2000)

    # Open the input file with gzip
    wiki=GzipFile(input_file)

    # Instantiate a CirrusSearch instance, pass our parser's 
    # input queue put function to be used as a callback 
    reader=CirrusSearchReader(input_queue.put)

    # Start the status monitor printout
    status=Thread(
        target=io_funcs.display_status, 
        args=(input_queue, output_queue, reader)
    )

    status.start() 

    # Start 15 parser jobs
    for _ in range(15):

        process=Process(
            target=parse_funcs.parse_cirrussearch_article, 
            args=(input_queue, output_queue, index_name)
        )

        process.start()

    # Target the correct output function

    # Save to file
    if output_destination == 'file':

        write_thread=Thread(
            target=io_funcs.write_file, 
            args=(output_queue)
        )

    # Insert to OpenSearch
    elif output_destination == 'opensearch':

        write_thread=Thread(
            target=io_funcs.bulk_index_articles, 
            args=(output_queue, index_name)
        )

    # Not sure what to do - warn user
    else:
        print(f'Unrecognized output destination: {output_destination}.')

    # Start the output writer thread
    write_thread.start()

    # Send the XML data stream to the reader via xml's sax parser
    for line in wiki:

        # Convert line to dict
        line=json.loads(line)

        # If a line is an index line
        if 'index' in line.keys():

            # Make some updates to make it compatible with OpenSearch
            line = parse_funcs.update_cs_index(line, index_name, id_num)

            # Increment the id num for next time: since each article inserted
            # has an index dict and a content dict, only update when we
            # find an index dict to get the correct count
            id_num+=1

        # Add to batch
        batch.append(line)

        # Once we have 1000 lines, write to chosen output
        if len(batch) == 1000:
            
            _=client.bulk(batch)

            # Clear the batch to collect the next
            batch = []

            # Count it
            batch_count+=1
            print(f'Batch {batch_count} inserted', end='\r')
