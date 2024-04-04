from multiprocessing import Manager, Process
from bz2 import BZ2File
from threading import Thread
from xml import sax

from wikisearch.classes.wikireader import WikiReader
import wikisearch.functions.parse_helper_functions as helper_funcs

def run():
    '''Main function to run XML dump parse'''

    # Flag to track if we are done
    shutdown = False
    
    # Start multiprocessing manager
    manager = Manager()

    # Set-up queues
    output_queue = manager.Queue(maxsize=2000)
    input_queue = manager.Queue(maxsize=2000)
    
    # Open bzip data stream from XML dump file
    wiki = BZ2File('wikisearch/data/enwiki-20240320-pages-articles-multistream.xml.bz2')

    # Instantiate a WikiReader instance, pass it a lambda function
    # to filter record namespaces and our article queue's put to
    # be used as a callback
    reader = WikiReader(lambda ns: ns == 0, input_queue.put)

    status = Thread(target=helper_funcs.display_status, args=(input_queue, output_queue, reader))
    status.start() 

    for _ in range(15):
        process = Process(target=helper_funcs.parse_article, args=(input_queue, output_queue, shutdown))
        process.start()

    write_thread = Thread(target=helper_funcs.write_out, args=(output_queue, shutdown))
    write_thread.start()

    sax.parse(wiki, reader)
    shutdown = True