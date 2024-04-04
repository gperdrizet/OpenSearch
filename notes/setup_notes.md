# Wikipedia and OpenSearch setup notes

Plan is to get the full text of wikipedia and load it into OpenSearch for use in other projects. From what I have read, this should be surprisingly easy. OpenSearch is available as a docker image and wikipedia publishes production elasticsearch indices weekly. Let's give it a shot:

## Terminology

1. Elasticsearch: Distributed, multitenant-capable full-text search engine with an HTTP web interface
2. OpenSearch: open source fork of Elasticsearch created by AWS in 2021
3. CirrusSearch: MediaWiki extension implementing Elasticsearch
4. MediaWiki: Wiki engine that runs wikipedia and others

## Download wikipedia

Wikipedia seems to publish updated dumps every few days with a somewhat irregular frequency. The CirrusSearch indexes are available at <https://dumps.wikimedia.org/other/cirrussearch/>. The contents of that directory on 2024-04-03 are as follows:

```text
Index of /other/cirrussearch/
../
20240219/   21-Feb-2024 12:08    -
20240226/   05-Mar-2024 13:09    -
20240304/   11-Mar-2024 09:29    -
20240305/   13-Mar-2024 07:58    -
20240311/   18-Mar-2024 12:05    -
20240313/   21-Mar-2024 03:12    -
20240318/   25-Mar-2024 16:12    -
20240321/   25-Mar-2024 16:32    -
20240325/   02-Apr-2024 13:38    -
20240401/   03-Apr-2024 14:08    -
20240402/   02-Apr-2024 13:38    -
20240403/   03-Apr-2024 11:11    -
current/    03-Apr-2024 14:08    -
```

Get the cirrussearch indexes for the article pages:

```text
$ wget https://dumps.wikimedia.org/other/cirrussearch/current/enwiki-20240325-cirrussearch-content.json.gz
$ gunzip enwiki-20240325-cirrussearch-content.json.gz
$ du -sh ./*

151G     ./enwiki-20240325-cirrussearch-content.json
37G     ./enwiki-20240325-cirrussearch-content.json.gz
```

OK, looks like we got it. Might be a pain to keep updated - the whole thing comes down as one giant JSON file. But, OK, declaring victory for now.

## Get OpenSearch

Get the docker compose file:

```text
wget https://raw.githubusercontent.com/opensearch-project/documentation-website/2.12/assets/examples/docker-compose.yml
```

Here's what the docker-compose file looks like:

```yaml
version: '3'
services:
  opensearch-node1: # This is also the hostname of the container within the Docker network (i.e. https://opensearch-node1/)
    image: opensearchproject/opensearch:latest
    container_name: opensearch-node1
    environment:
      - cluster.name=opensearch-cluster # Name the cluster
      - node.name=opensearch-node1 # Name the node that will run in this container
      - discovery.seed_hosts=opensearch-node1,opensearch-node2 # Nodes to look for when discovering the cluster
      - cluster.initial_cluster_manager_nodes=opensearch-node1,opensearch-node2 # Nodes eligibile to serve as cluster manager
      - bootstrap.memory_lock=true # Disable JVM heap memory swapping
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" # Set min and max JVM heap sizes to at least 50% of system RAM
      - OPENSEARCH_INITIAL_ADMIN_PASSWORD=${OPENSEARCH_INITIAL_ADMIN_PASSWORD} # Sets the demo admin user password when using demo configuration (for OpenSearch 2.12 and later)
    ulimits:
      memlock:
        soft: -1 # Set memlock to unlimited (no soft or hard limit)
        hard: -1
      nofile:
        soft: 65536 # Maximum number of open files for the opensearch user - set to at least 65536
        hard: 65536
    volumes:
      - opensearch-data1:/usr/share/opensearch/data # Creates volume called opensearch-data1 and mounts it to the container
    ports:
      - 9200:9200 # REST API
      - 9600:9600 # Performance Analyzer
    networks:
      - opensearch-net # All of the containers will join the same Docker bridge network
  opensearch-node2:
    image: opensearchproject/opensearch:latest # This should be the same image used for opensearch-node1 to avoid issues
    container_name: opensearch-node2
    environment:
      - cluster.name=opensearch-cluster
      - node.name=opensearch-node2
      - discovery.seed_hosts=opensearch-node1,opensearch-node2
      - cluster.initial_cluster_manager_nodes=opensearch-node1,opensearch-node2
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
      - OPENSEARCH_INITIAL_ADMIN_PASSWORD=${OPENSEARCH_INITIAL_ADMIN_PASSWORD}
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    volumes:
      - opensearch-data2:/usr/share/opensearch/data
    networks:
      - opensearch-net
  opensearch-dashboards:
    image: opensearchproject/opensearch-dashboards:latest # Make sure the version of opensearch-dashboards matches the version of opensearch installed on other nodes
    container_name: opensearch-dashboards
    ports:
      - 5601:5601 # Map host port 5601 to container port 5601
    expose:
      - "5601" # Expose port 5601 for web access to OpenSearch Dashboards
    environment:
      OPENSEARCH_HOSTS: '["https://opensearch-node1:9200","https://opensearch-node2:9200"]' # Define the OpenSearch nodes that OpenSearch Dashboards will query
    networks:
      - opensearch-net

volumes:
  opensearch-data1:
  opensearch-data2:

networks:
  opensearch-net:
  
```

Starts (or tries to start) two node containers - both fail because *OPENSEARCH_INITIAL_ADMIN_PASSWORD* has not been set. Relevant line from *docker-compose.yaml*:

```yaml
- OPENSEARCH_INITIAL_ADMIN_PASSWORD=${OPENSEARCH_INITIAL_ADMIN_PASSWORD}
```

Let's set it as an environment variable in the host system via a venv (we are gonna end up using python anyway so might as well). Make and update a venv:

```text
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
```

Add the following to *.venv/bin/activate*:

```text
export OPENSEARCH_INITIAL_ADMIN_PASSWORD=some_good_password
```

OK - no matter what password we set, still getting a warning and the containers refuse to start. Turn security off in docker-compose:

```text
environment:
   - DISABLE_SECURITY_PLUGIN=true
```

Better. No more password warning. Now we get a new error in the containers:

```text
ERROR: [1] bootstrap checks failed
[1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
ERROR: OpenSearch did not exit normally - check the logs at /usr/share/opensearch/logs/opensearch-cluster.log
```

Add the following to */etc/sysctl.conf*:

```text
vm.max_map_count=262144
```

Disable swap:

```text
 sudo swapoff -a
```

Then reload the kernel parameters and try again:

```text
sudo sysctl -p
docker-compose up -d
```

OK - Looks good. Let the containers marinate for a few minutes. Lots of log output, nothing concerning - everything seems to be running. Here is docker ps:

```text
$ docker ps

CONTAINER ID   IMAGE                                            COMMAND                  CREATED          STATUS          PORTS                                                                                                      NAMES
8d15a5a29969   opensearchproject/opensearch:latest              "./opensearch-docker…"   9 minutes ago    Up 2 minutes    0.0.0.0:9200->9200/tcp, :::9200->9200/tcp, 9300/tcp, 0.0.0.0:9600->9600/tcp, :::9600->9600/tcp, 9650/tcp   opensearch-node1
9799856ce6b1   opensearchproject/opensearch:latest              "./opensearch-docker…"   9 minutes ago    Up 2 minutes    9200/tcp, 9300/tcp, 9600/tcp, 9650/tcp                                                                     opensearch-node2
ff8a2bec145e   opensearchproject/opensearch-dashboards:latest   "./opensearch-dashbo…"   36 minutes ago   Up 36 minutes   0.0.0.0:5601->5601/tcp, :::5601->5601/tcp                                                                  opensearch-dashboards
```

Cool - not great that the quickstart guide doesn't work without modification, and we definitely don't want to run with the security plugin disabled for ever. But I think we are gonna call it OK for testing on the private LAN for now. Let's keep going and we will come back to it later.

Next, want eyes on the dashboard - since this is running on pyrite, *LOCALHOST* is not going to work, we need it accessible at least on the LAN. The docs point to */usr/share/opensearch-dashboards/config/opensearch_dashboards.yml* for docker:

... Anyone? holy crap the documentation is terrible. After a bit of floundering around, I found [this](https://opensearch.org/docs/2.12/install-and-configure/install-opensearch/docker/):

```text
If you override opensearch_dashboards.yml settings using environment variables in your compose file, use all uppercase letters and replace periods with underscores (for example, for opensearch.hosts, use OPENSEARCH_HOSTS). This behavior is inconsistent with overriding opensearch.yml settings, where the conversion is just a change to the assignment operator (for example, discovery.type: single-node in opensearch.yml is defined as discovery.type=single-node in docker-compose.yml).
```

Yikes, OK, let's try it. Add the following to the *opensearch-dashboards* stanza in *docker-compose*:

```text
environment:
   SERVER_HOST: '192.168.1.148'
```

Still nope. Wait, I bet ufw is blocking the port(s)...

```text
sudo ufw allow 5001
sudo ufw allow 5601
```

WTF, still complaining about 5601 - but it's open:

```text
$ sudo ufw status

To                         Action      From
--                         ------      ----
22/tcp                     ALLOW       Anywhere                  
5001                       ALLOW       Anywhere                  
5601                       ALLOW       Anywhere                  
22/tcp (v6)                ALLOW       Anywhere (v6)             
5001 (v6)                  ALLOW       Anywhere (v6)             
5601 (v6)                  ALLOW       Anywhere (v6)
```

Whelp, think I fixed it - set *SERVER_HOST* to 0.0.0.0.... and...

From <http://192.168.1.148:5601/> we get:

```text
OpenSearch Dashboards server is not ready yet
```

I think that's progress - at least it's responding at all! Here is the next thing, the dashboard container log is polluted with this:

```text
{"type":"log","@timestamp":"2024-04-01T04:26:27Z","tags":["error","opensearch","data"],"pid":1,"message":"[ConnectionError]: write EPROTO 00E8E894327F0000:error:0A00010B:SSL routines:ssl3_get_record:wrong version number:../deps/openssl/openssl/ssl/record/ssl3_record.c:355:\n"}
```

Added the following in the node environment stanzas:

```text
   - server.ssl.enabled=false
```

And in the dashboard environment stanza:

```text
   SERVER_SSL_ENABLED: 'false'
```

Now we get a new error:

```text
{"type":"log","@timestamp":"2024-04-01T04:39:02Z","tags":["error","opensearch","data"],"pid":1,"message":"[ConnectionError]: connect ECONNREFUSED 172.18.0.2:9200"}
```

Weird - what the heck is 172.128.0.2? OK - looks like that's opensearch-node1 on the docker opensearch network. So the dashboard can't talk to the nodes. Let's take a look at the logs again.

Yep, now we have an error in the node logs:

```text
[2024-04-03T16:10:52,399][ERROR][o.o.b.OpenSearchUncaughtExceptionHandler] [opensearch-node1] uncaught exception in thread [main]
org.opensearch.bootstrap.StartupException: java.lang.IllegalArgumentException: unknown setting [server.ssl.enabled] please check that any required plugins are installed, or check the breaking changes documentation for removed settings
```

Looks like setting *server.ssl.enabled=False* is causing a problem. Let's remove that line from docker-compose.yaml and see how we do.

Now we are going around in circles. The node containers no longer throw errors, but the dashboard container is back to complaining about *ssl3_get_record:wrong version number* just like it did before we set *server.ssl.enabled=False* in the nodes.

After some deeper reading in the documentation I found that we have two options in regard to security;

1. Set up and use a demo config to get started
2. Remove the security plugin from the nodes and the dashboard and run without it

Apparently the instructions in the 'quickstart' portion of the documentation doesn't really do either correctly. Here is a functioning docker-compose.yaml [from a bit deeper into the docs](https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/).

```text
version: '3'
services:
  opensearch-node1:
    image: opensearchproject/opensearch:latest
    container_name: opensearch-node1
    environment:
      - cluster.name=opensearch-cluster # Name the cluster
      - node.name=opensearch-node1 # Name the node that will run in this container
      - discovery.seed_hosts=opensearch-node1,opensearch-node2 # Nodes to look for when discovering the cluster
      - cluster.initial_cluster_manager_nodes=opensearch-node1,opensearch-node2 # Nodes eligibile to serve as cluster manager
      - bootstrap.memory_lock=true # Disable JVM heap memory swapping
      - "OPENSEARCH_JAVA_OPTS=-Xms64g -Xmx64g" # Set min and max JVM heap sizes to at least 50% of system RAM
      - "DISABLE_INSTALL_DEMO_CONFIG=true" # Prevents execution of bundled demo script which installs demo certificates and security configurations to OpenSearch
      - "DISABLE_SECURITY_PLUGIN=true" # Disables Security plugin
    ulimits:
      memlock:
        soft: -1 # Set memlock to unlimited (no soft or hard limit)
        hard: -1
      nofile:
        soft: 65536 # Maximum number of open files for the opensearch user - set to at least 65536
        hard: 65536
    volumes:
      - opensearch-data1:/usr/share/opensearch/data # Creates volume called opensearch-data1 and mounts it to the container
    ports:
      - 9200:9200 # REST API
      - 9600:9600 # Performance Analyzer
    networks:
      - opensearch-net # All of the containers will join the same Docker bridge network
  opensearch-node2:
    image: opensearchproject/opensearch:latest
    container_name: opensearch-node2
    environment:
      - cluster.name=opensearch-cluster # Name the cluster
      - node.name=opensearch-node2 # Name the node that will run in this container
      - discovery.seed_hosts=opensearch-node1,opensearch-node2 # Nodes to look for when discovering the cluster
      - cluster.initial_cluster_manager_nodes=opensearch-node1,opensearch-node2 # Nodes eligibile to serve as cluster manager
      - bootstrap.memory_lock=true # Disable JVM heap memory swapping
      - "OPENSEARCH_JAVA_OPTS=-Xms64g -Xmx64g" # Set min and max JVM heap sizes to at least 50% of system RAM
      - "DISABLE_INSTALL_DEMO_CONFIG=true" # Prevents execution of bundled demo script which installs demo certificates and security configurations to OpenSearch
      - "DISABLE_SECURITY_PLUGIN=true" # Disables Security plugin
    ulimits:
      memlock:
        soft: -1 # Set memlock to unlimited (no soft or hard limit)
        hard: -1
      nofile:
        soft: 65536 # Maximum number of open files for the opensearch user - set to at least 65536
        hard: 65536
    volumes:
      - opensearch-data2:/usr/share/opensearch/data # Creates volume called opensearch-data2 and mounts it to the container
    networks:
      - opensearch-net # All of the containers will join the same Docker bridge network
  opensearch-dashboards:
    image: opensearchproject/opensearch-dashboards:latest
    container_name: opensearch-dashboards
    ports:
      - 5601:5601 # Map host port 5601 to container port 5601
    expose:
      - "5601" # Expose port 5601 for web access to OpenSearch Dashboards
    environment:
      - 'OPENSEARCH_HOSTS=["http://opensearch-node1:9200","http://opensearch-node2:9200"]'
      - "DISABLE_SECURITY_DASHBOARDS_PLUGIN=true" # disables security dashboards plugin in OpenSearch Dashboards
    networks:
      - opensearch-net

volumes:
  opensearch-data1:
  opensearch-data2:

networks:
  opensearch-net:
```

This is a much better starting point from which we can do a better job of setting up the security later if need be.

OK, so dashboard is up at <http://0.0.0.0:5601> on the LAN and the nodes are running with clean logs.

Only additional change is to increase "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" to 64GB and restart the containers.

## Import enwiki

Working off of a hint from a [stackoverflow thread](https://stackoverflow.com/questions/47476122/loading-wikipedia-dump-into-elasticsearch)

The advice is as follows:

```text
create a mapping according your needs. For example:

{
   "mappings": {
     "page": {
        "properties": {
           "auxiliary_text": {
              "type": "text"
           },
           "category": {
              "type": "text"
           },
           "coordinates": {
              "properties": {
                 "coord": {
                    "properties": {
                       "lat": {
                          "type": "double"
                       },
                       "lon": {
                          "type": "double"
                       }
                    }
                 },
                 "country": {
                    "type": "text"
                 },
                 "dim": {
                    "type": "long"
                 },
                 "globe": {
                    "type": "text"
                 },
                 "name": {
                    "type": "text"
                 },
                 "primary": {
                    "type": "boolean"
                 },
                 "region": {
                    "type": "text"
                 },
                 "type": {
                    "type": "text"
                 }
              }
           },
           "defaultsort": {
              "type": "boolean"
           },
           "external_link": {
              "type": "text"
           },
           "heading": {
              "type": "text"
           },
           "incoming_links": {
              "type": "long"
           },
           "language": {
              "type": "text"
           },
           "namespace": {
              "type": "long"
           },
           "namespace_text": {
              "type": "text"
           },
           "opening_text": {
              "type": "text"
           },
           "outgoing_link": {
              "type": "text"
           },
           "popularity_score": {
              "type": "double"
           },
           "redirect": {
              "properties": {
                 "namespace": {
                    "type": "long"
                 },
                 "title": {
                    "type": "text"
                 }
              }
           },
           "score": {
              "type": "double"
           },
           "source_text": {
              "type": "text"
           },
           "template": {
              "type": "text"
           },
           "text": {
              "type": "text"
           },
           "text_bytes": {
              "type": "long"
           },
           "timestamp": {
              "type": "date",
              "format": "strict_date_optional_time||epoch_millis"
           },
           "title": {
              "type": "text"
           },
           "version": {
              "type": "long"
           },
           "version_type": {
              "type": "text"
           },
           "wiki": {
              "type": "text"
           },
           "wikibase_item": {
              "type": "text"
           }
        }
     }
  }
}

once you have created the index you just type:

zcat enwiki-current-cirrussearch-general.json.gz | parallel --pipe -L 2 -N 2000 -j3 'curl -s http://localhost:9200/enwiki/_bulk --data-binary @- > /dev/null'

```

Yikes! That's like a 150 GB one liner! Hopefully, it will be enough to get us going. Probably will end up doing it via python? I dunno.

Let's give it a shot. I already unzipped the json dump, so change that but otherwise the same:

```text
cat enwiki-20240325-cirrussearch-content.json | parallel --pipe -L 2 -N 2000 -j3 'curl -s http://localhost:9200/enwiki/_bulk --data-binary @- > /dev/null'
```

Looks like it's going. Had ti install parallel with:

```text
sudo apt install parallel
```

After that, our cat command produced a few lines of:

```text
parallel: Warning: A record was longer than 1048576. Increasing to --blocksize 1363150.
parallel: Warning: A record was longer than 1363150. Increasing to --blocksize 1772096.
parallel: Warning: A record was longer than 1772096. Increasing to --blocksize 2303726.
```

And then started humming away. Checking the interfaces with bmon tells me there is data moving: bond0 is transmitting ~120 MB/sec from the RAID array (where the JSON file is located) and lo is seeing concurrent RX TX of 100-130 MB/sec. We can do sequential read from the array faster than that via the bond, but I think lo is limiting the transfer speed? Weird, quick search says that probably should not be the case. Not sure what to think about that. For now I'm OK with being limited a just better than gigabit speed for this transfer. Probably won't be doing it that often anyway. Looks like it's using about 12 GB system memory and only a few percent of CPU. So, for 150 GB at ~120 MB/sec., should take like 20 min.

OK, yep - about 15 min and it finished cleanly - no additional output from the cat command.

## Test out python client

OK, let's see if we can search from a python script. From our venv:

```text
pip install opensearch-py
```

Then run the following to see the indexes currently in OpenSearch:

```python
# search.py

from opensearchpy import OpenSearch

host='localhost'
port=9200

# Create the client with SSL/TLS and hostname verification disabled.
client=OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True, # enables gzip compression for request bodies
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

response=client.indices.get('_all')

print()

for key, val in response.items():
    print(f'{key}')

print()
```

```text
$ python search.py

.kibana_1
.opensearch-observability
.plugins-ml-config
.ql-datasources

```

OK, well - I don't see our data. I think we maybe should load the data in with python? Guessing we do have to create an index first. Problem there is - we have a 150 GB JSON file that definitely won't fit in system memory. Sounds like we can use ijson to stream it like this:

```python
import ijson

# Open the JSON file
with open('large_file.json', 'r') as file:
    # Parse the JSON objects one by one
    parser=ijson.items(file, 'item')
    
    # Iterate over the JSON objects
    for item in parser:
        # Process each JSON object as needed
        print(item)
```

No joy - ijson complains about garbage in the file. Think we have two options here:

1. Get the dump in XML or SQL and process it ourselves - this will give the most control but will also probably be a pain in the butt. There are a large number of files/formats avalible and figuring out what to download and how to use it would take some effort.
2. Learn how to properly use the CirrusSearch dump

After some quick reading, option one seems tractable - we would have to write and xml parser and probably parallelize it - similarly to how we build the PubSum SQL database. There is more writing on this than the CirrusSearch dumps. See the following:

1. <https://jamesthorne.com/blog/processing-wikipedia-in-a-couple-of-hours>
2. <https://dev.to/tobiasjc/understanding-the-wikipedia-dump-11f1>

I sort of prefer the idea of using CirrusSearch dumps. Seems silly to re-invent the wheel and basically rebuild the whole thing when ostensibly it's right there to be imported, but I can't seem to find enough info to even get started. All I have is that one stackoverflow thread.

OK, after a little more reading - we are going with option 1. I really like the James Thorne article, there are some very cool concepts and good use of queues and threading etc. I want to implement it just to get some practice with those concepts. Much cleaner and more effective than my PubMed XML parser. Also, having my own parser will give me a lot more control over what/how I build my database and also the possibility to include things for which there aren't published CirrusSearch dumps.

At this point, I think we should start a project repo and I will continue my XML parser notes in a different file.
