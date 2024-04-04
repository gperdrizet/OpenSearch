from xml.sax import ContentHandler
import logging

logger = logging.getLogger(__name__)

class WikiReader(ContentHandler):
    def __init__(self, ns_filter, callback):
        super().__init__()

        self.filter = ns_filter
        
        self.read_stack = []
        self.read_text = None
        self.read_title = None
        self.read_namespace = None
        
        self.status_count = 0
        self.callback = callback


    def startElement(self, tag_name, attributes):
        if tag_name == "ns":
            self.read_namespace = None
            
        elif tag_name == "page":
            self.read_text = None
            self.read_title = None
            
        elif tag_name == "title":
            self.read_title = ""
            
        elif tag_name == "text":
            self.text_text = ""
            
        else:
            return

        self.read_stack.append(tag_name)


    def endElement(self, tag_name):
        if tag_name == self.read_stack[-1]:
            del self.read_stack[-1]

        if self.filter(self.read_namespace):
            if tag_name == "page" and self.read_text is not None:
                self.status_count += 1
                self.callback((self.read_title, self.read_text))
                

    def characters(self, content):
        if len(self.read_stack) == 0:
            return

        if self.stack[-1] == "text":
            self.text += content

        if self.stack[-1] == "title":
            self.title += content

        if self.stack[-1] == "ns":
            self.ns = int(content)