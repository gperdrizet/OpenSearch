import json

class CirrusSearchReader():
    '''Class to XML objects from CirrusSearch dump'''
    
    def __init__(self, callback):

        # Add parser queue callback function
        self.callback=callback

        # Buffer to accumulate header and content before
        # sending to parser's input queue
        self.buffer = []

        # Start article count
        self.status_count=0

    def read_line(self, line):
        
        # Convert line to dict
        line=json.loads(line)

        # Add it to the buffer
        self.buffer.append(line)

        # If we have two lines in the buffer
        # flush it
        if len(self.buffer) == 2:
            self.flush_buffer()

    def flush_buffer(self):

        # Add the article number and put the buffer 
        # contents into the parser input queue
        self.buffer.append(self.status_count)
        self.callback(self.buffer)

        # Clear the buffer
        self.buffer = []

        # Update article count
        self.status_count += 1