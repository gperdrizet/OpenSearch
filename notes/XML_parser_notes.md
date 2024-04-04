# Wikipedia XML dump parser build notes

Plan is to get the full text of Wikipedia as an XML dump, then parse it and insert it into a OpenSearch vector database.

First, pull the data. Using the Clarkson.edu mirror:

```text
wget https://wikimedia.mirror.clarkson.edu/enwiki/20240320/enwiki-20240320-pages-articles-multistream.xml.bz2
```
