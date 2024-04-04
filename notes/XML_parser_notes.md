# Wikipedia XML dump parser build notes

Plan is to get the full text of Wikipedia as an XML dump, then parse it and insert it into a OpenSearch vector database.

First, pull the data. Using the Clarkson.edu mirror:

```text
$ wget https://wikimedia.mirror.clarkson.edu/enwiki/20240320/enwiki-20240320-pages-articles-multistream.xml.bz2
$ du -sh ./*

22G     ./enwiki-20240320-pages-articles-multistream.xml.bz2
```

OK, got 22 GB - this is gonna be a bit of a pain to keep updated, but let's not worry about that yet - we probably won't need/want to do it very often. This file could probably fit in system memory uncompressed but the idea is to use python's bz2 module to stream it into xml's sax parser. This approach was heavily inspired by a [blog post](https://jamesthorne.com/blog/processing-wikipedia-in-a-couple-of-hours) by James Thorne.
