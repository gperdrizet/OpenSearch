from xml.sax import ContentHandler
import logging

logger=logging.getLogger(__name__)

class WikiReader(ContentHandler):
    '''Class to read tags parsed from bz2 data stream 
    via xml.sax's parser'''
    
    def __init__(self, ns_filter, callback):
        super().__init__()

        # Add namespace filter and parser queue callback function
        self.filter=ns_filter
        self.callback=callback
        
        # Set some initial values
        self.read_stack= []
        self.read_text=None
        self.read_title=None
        self.read_namespace=None
        
        # Start article count
        self.status_count=0


    def startElement(self, tag_name, attributes):
        '''Takes tag name and stores content, adds to stack'''

        # Decide what to do based on the tag name

        # Empty namespace for new value
        if tag_name == 'ns':
            self.read_namespace=None
        
        # If we found a page empty the title and text
        # for new values
        elif tag_name == 'page':
            self.read_text=None
            self.read_title=None
        
        # If we found a title, empty the title string
        elif tag_name == 'title':
            self.read_title=''
        
        # If we found text, empty the text string
        elif tag_name == 'text':
            self.read_text=''
            
        # If it's none of those, do nothing
        else:
            return

        # Add the tag to the stack
        self.read_stack.append(tag_name)


    def endElement(self, tag_name):
        '''Takes tag name, routes based on identity'''

        # If we decided not to save this tag, do nothing
        if len(self.read_stack) == 0:
            return
        
        # If this tag matches the stack, delete
        # and process further
        if tag_name == self.read_stack[-1]:
            del self.read_stack[-1]

        # If it's a page tag
        if tag_name == "page":

            # That has text
            if self.read_text is not None:

                # And has namespace 0 (article)
                if self.read_namespace == 0:

                    # Call the callback to add the article title and text
                    # to the parser's input queue
                    self.callback((self.read_title, self.read_text))

                    # Count
                    self.status_count += 1
                

    def characters(self, content):
        '''Handles tag content'''

        # If we decided not to save this tag, also skip
        # the content
        if len(self.read_stack) == 0:
            return

        # If the tag is text, add the content
        if self.read_stack[-1] == 'text':
            self.read_text += content

        # If the tag is title, add the title
        if self.read_stack[-1] == 'title':
            self.read_title += content

        # If it's a namespace tag, update the namespace
        if self.read_stack[-1] == 'ns':
            self.read_namespace = int(content)