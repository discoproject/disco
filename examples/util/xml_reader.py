import xml.sax
import threading
from Queue import Queue
from disco.core import Job, result_iterator
from disco.worker.classic.func import chain_reader

"""
For using this example, you should obtain an sml corpus and do the following:

1. Add the current directory to the python path
    $ export PYTHONPATH=$PYTHONPATH:.

2. For the xml file, we will use a very small portion of wikipedia dump:
    $ wget --no-check-certificate https://raw.githubusercontent.com/pooya/discostuff/master/sample.xml

(The actual wikipedia dump can be found here (very large file):
http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2)

3. Use the xml_reader function in this file to extract the desried tags:
    $ ddfs chunk data:xml:read ./sample.xml --reader xml_reader.xml_reader

   for the wikipedia corpus, the desired tag is "text"

4. Then you can run any job that uses the content of this ddfs tag.
    $ python xml_reader.py
"""

XML_TAG = "text"
DDFS_TAG = "data:xml:read"
QUEUE_SIZE = 8192

class ABContentHandler(xml.sax.ContentHandler):
    def __init__(self, q):
        xml.sax.ContentHandler.__init__(self)
        self.q = q

    def startElement(self, name, attrs):
        self.tag = name
        self.content = ""

    def endElement(self, name):
        if self.tag == XML_TAG:
            self.q.put(self.content)
        self.tag = ""

    def characters(self, content):
        if self.tag == XML_TAG:
            self.content += content

class ReadProxy(object):
    def __init__(self, stream, q):
        self.stream = stream
        self.buffer = ""
        self.q = q

    def read(self, size):
        if self.buffer == "":
            try:
                self.buffer = self.stream.next()
            except:
                self.q.put(0)
                return ""
        if size < len(self.buffer):
            buffer = self.buffer[:size]
            self.buffer = self.buffer[size:]
            return buffer
        else:
            return self._read()

    def _read(self):
        buffer = self.buffer
        self.buffer = ""
        return buffer

def xml_reader(stream, size, url, params):
    q = Queue(QUEUE_SIZE)
    xml_reader.rproxy = ReadProxy(stream, q)
    threading.Thread(target=lambda q: xml.sax.parse(xml_reader.rproxy, ABContentHandler(q)),
                     args=(q,)).start()
    while True:
        item = q.get()
        if item == 0:
            return
        yield item
        q.task_done()

def map(line, params):
    import __builtin__
    unwanted = u",!.#()][{}-><=|/\"'*:?"
    words = line.translate(__builtin__.dict.fromkeys([ord(x) for x in
        unwanted], u" ")).lower()
    for word in words.split():
        yield word, 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':
    job = Job().run(input=["tag://" + DDFS_TAG],
                    map=map,
                    reduce=reduce,
                    map_reader = chain_reader)

    for line, count in result_iterator(job.wait(show=True)):
        print(line, count)
