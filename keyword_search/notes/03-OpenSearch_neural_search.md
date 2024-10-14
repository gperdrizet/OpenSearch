# OpenSearch neural search tutorial

Set-up for neural/semantic search according to OpenSearch's [tutorial](https://opensearch.org/docs/latest/search-plugins/neural-search-tutorial/).

## 1. Cluster set-up

Since we don't have a dedicated ML node and the cluster is running all on the same machine send the following via the OpenSearch console. The console can be found on in the OpenSearch dashboard under dev tools.

```text
PUT _cluster/settings
{
  "persistent": {
    "plugins": {
      "ml_commons": {
        "only_run_on_ml_node": "false",
        "model_access_control_enabled": "true",
        "native_memory_threshold": "99"
      }
    }
  }
}
```

Response:

```text
{
  "acknowledged": true,
  "persistent": {
    "plugins": {
      "ml_commons": {
        "only_run_on_ml_node": "false",
        "model_access_control_enabled": "true",
        "native_memory_threshold": "99"
      }
    }
  },
  "transient": {}
}
```

## 2. Set up the embedding model

Use the DistilBERT model as described in the tutorial as a starting point.

### 2.1. Register a model group

Omit the access control parameter because we are running with the security plugin disabled.

```text
POST /_plugins/_ml/model_groups/_register
{
  "name": "NLP_model_group",
  "description": "A model group for NLP models"
}
```

Response:

```text
{
  "model_group_id": "naFmWpIBgWKaVuCyTvjw",
  "status": "CREATED"
}
```

### 2.2. Register the model

```text
POST /_plugins/_ml/models/_register
{
  "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
  "version": "1.0.1",
  "model_group_id": "naFmWpIBgWKaVuCyTvjw",
  "model_format": "TORCH_SCRIPT"
}
```

```text
{
  "task_id": "n6FpWpIBgWKaVuCy2viU",
  "status": "CREATED"
}
```

Check the task status with:

```text
GET /_plugins/_ml/tasks/n6FpWpIBgWKaVuCy2viU
```

Once the task is complete, you should receive output like this:

```text
{
  "model_id": "oKFpWpIBgWKaVuCy3vi8",
  "task_type": "REGISTER_MODEL",
  "function_name": "TEXT_EMBEDDING",
  "state": "COMPLETED",
  "worker_node": [
    "DVPXlmoKSCKKO3K0G0CL3g"
  ],
  "create_time": 1728093739666,
  "last_update_time": 1728093777611,
  "is_async": true
}
```

Check that the model was loaded correctly:

```text
GET /_plugins/_ml/models/oKFpWpIBgWKaVuCy3vi8
```

Response:

```text
{
  "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
  "model_group_id": "naFmWpIBgWKaVuCyTvjw",
  "algorithm": "TEXT_EMBEDDING",
  "model_version": "1",
  "model_format": "TORCH_SCRIPT",
  "model_state": "REGISTERED",
  "model_content_size_in_bytes": 266352827,
  "model_content_hash_value": "acdc81b652b83121f914c5912ae27c0fca8fabf270e6f191ace6979a19830413",
  "model_config": {
    "model_type": "distilbert",
    "embedding_dimension": 768,
    "framework_type": "SENTENCE_TRANSFORMERS",
    "all_config": """{"_name_or_path":"old_models/msmarco-distilbert-base-tas-b/0_Transformer","activation":"gelu","architectures":["DistilBertModel"],"attention_dropout":0.1,"dim":768,"dropout":0.1,"hidden_dim":3072,"initializer_range":0.02,"max_position_embeddings":512,"model_type":"distilbert","n_heads":12,"n_layers":6,"pad_token_id":0,"qa_dropout":0.1,"seq_classif_dropout":0.2,"sinusoidal_pos_embds":false,"tie_weights_":true,"transformers_version":"4.7.0","vocab_size":30522}"""
  },
  "created_time": 1728093740512,
  "last_updated_time": 1728093777569,
  "last_registered_time": 1728093777568,
  "total_chunks": 27,
  "is_hidden": false
}
```

### 2.3. Deploy the model

Deploying loads a model instance into memory so that it is ready to use.

```text
POST /_plugins/_ml/models/oKFpWpIBgWKaVuCy3vi8/_deploy
```

Response:

```text
{
  "task_id": "oaFyWpIBgWKaVuCyS_hW",
  "task_type": "DEPLOY_MODEL",
  "status": "CREATED"
}
```

Check the task status:

```text
GET /_plugins/_ml/tasks/oaFyWpIBgWKaVuCyS_hW
```

Response:

```text
{
  "model_id": "oKFpWpIBgWKaVuCy3vi8",
  "task_type": "DEPLOY_MODEL",
  "function_name": "TEXT_EMBEDDING",
  "state": "COMPLETED",
  "worker_node": [
    "rMLZQSO-Sk-o-KVCe2VAHA",
    "DVPXlmoKSCKKO3K0G0CL3g",
    "L4Vjp5m3Toik7jyj09ezRg"
  ],
  "create_time": 1728094292820,
  "last_update_time": 1728094308356,
  "is_async": true
}
```

The model state should now show up as 'deployed':

```text
{
  "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
  "model_group_id": "naFmWpIBgWKaVuCyTvjw",
  "algorithm": "TEXT_EMBEDDING",
  "model_version": "1",
  "model_format": "TORCH_SCRIPT",
  "model_state": "DEPLOYED",
  "model_content_size_in_bytes": 266352827,
  "model_content_hash_value": "acdc81b652b83121f914c5912ae27c0fca8fabf270e6f191ace6979a19830413",
  "model_config": {
    "model_type": "distilbert",
    "embedding_dimension": 768,
    "framework_type": "SENTENCE_TRANSFORMERS",
    "all_config": """{"_name_or_path":"old_models/msmarco-distilbert-base-tas-b/0_Transformer","activation":"gelu","architectures":["DistilBertModel"],"attention_dropout":0.1,"dim":768,"dropout":0.1,"hidden_dim":3072,"initializer_range":0.02,"max_position_embeddings":512,"model_type":"distilbert","n_heads":12,"n_layers":6,"pad_token_id":0,"qa_dropout":0.1,"seq_classif_dropout":0.2,"sinusoidal_pos_embds":false,"tie_weights_":true,"transformers_version":"4.7.0","vocab_size":30522}"""
  },
  "created_time": 1728093740512,
  "last_updated_time": 1728094308356,
  "last_registered_time": 1728093777568,
  "last_deployed_time": 1728094308356,
  "auto_redeploy_retry_times": 0,
  "total_chunks": 27,
  "planning_worker_node_count": 3,
  "current_worker_node_count": 3,
  "planning_worker_nodes": [
    "rMLZQSO-Sk-o-KVCe2VAHA",
    "DVPXlmoKSCKKO3K0G0CL3g",
    "L4Vjp5m3Toik7jyj09ezRg"
  ],
  "deploy_to_all_nodes": true,
  "is_hidden": false
}
```

## 3. Data ingest

Now we need to start adapting this to our Wikipedia parse/insert code. This whole thing will need a refactor to make it modular. The ideal case would be to completely separate all the OpenSearch stuff from the Wikipedia specific stuff, that way it's reusable for other text sources.

### 3.1. Neural search ingest pipeline

The tutorial gives the following stanza for ingest pipeline creation:

```text
PUT /_ingest/pipeline/nlp-ingest-pipeline
{
  "description": "An NLP ingest pipeline",
  "processors": [
    {
      "text_embedding": {
        "model_id": "oKFpWpIBgWKaVuCy3vi8",
        "field_map": {
          "text": "embedding"
        }
      }
    }
  ]
}
```

This will tell the model to map the 'text' field from our incoming documents to the 'embedding' field in the index. We also need to chunk the documents into passages of 512 tokens, otherwise distillBERT chokes. We can do that by adding a text chunking processor to the ingest pipeline above. More information can be found in the [text chunking tutorial](https://opensearch.org/docs/latest/search-plugins/text-chunking/). Here is our updated input pipeline:

```text
PUT _ingest/pipeline/nlp-ingest-pipeline
{
  "description": "An NLP ingest pipeline",
  "processors": [
    {
      "text_chunking": {
        "algorithm": {
          "fixed_token_length": {
            "token_limit": 512,
            "overlap_rate": 0.2,
            "tokenizer": "standard"
          }
        },
        "field_map": {
          "text": "text_chunk"
        }
      }
    },
    {
      "text_embedding": {
        "model_id": "oKFpWpIBgWKaVuCy3vi8",
        "field_map": {
          "text_chunk": "text_chunk_embedding"
        }
      }
    }
  ]
}
```

Let's do that as part of index initialization (helper_funcs.initialize_index()). We also need to change how we are initializing the index. The tutorial gives the following example:

```text
PUT /my-nlp-index
{
  "settings": {
    "index": {
      "knn": true
    }
  },
  "mappings": {
    "properties": {
      "text": {
        "type": "text"
      },
      "text_chunk_embedding": {
        "type": "nested",
        "properties": {PUT /my-nlp-index
          "knn": {
            "type": "knn_vector",
            "dimension": 768
          }
        }
      }
    }
  }
}
```

We can easily adapt that by changing the 'id' field to 'title' to match what we are already doing. We also need to add a nested field for the chunked embeddings. Here is what our updated initialization stanza looks like:

```text
{
"settings": {
    "number_of_shards": 3,
    "index.knn": 'true',
    "default_pipeline": config.NLP_INGEST_PIPELINE_ID
},
"mappings": {
    "properties": {
        "title": {
            "type": "text"
        },
        "text": {
            "type": "text"
        },
        "text_chunk_embedding": {
            "type": "nested",
            "properties": {
                "knn": {
                    "type": "knn_vector",
                    "dimension": 768,
                    "method": {
                        "engine": "lucene",
                        "space_type": "l2",
                        "name": "hnsw",
                        "parameters": {}
                    }
                }
            }
        }
    }
}
}
```

Let's add that and a flag in configuration file to switch between that and the old way.

### 3.2. Add documents

Tutorial gives the following example command to add a document.

```text
PUT /my-nlp-index/_doc/1
{
  "text": "A West Virginia university women 's basketball team , officials , and a small gathering of fans are in a West Virginia arena .",
  "id": "4319130149.jpg"
}
```

All we need to do is adapt to our field names, and we are good to go.

## 4. Search

The tutorial describes a semantic only search and a hybrid keyword and semantic search. Let's set up a new test script using just the semantic search to start with.

```text
GET /my-nlp-index/_search
{
  "_source": {
    "excludes": [
      "passage_embedding"
    ]
  },
  "query": {
    "neural": {
      "passage_embedding": {
        "query_text": "wild west",
        "model_id": "aVeif4oB5Vm0Tdw8zYO2",
        "k": 5
      }
    }
  }
}
```

Translating that for our index:

```text
{
  "_source": {
    "excludes": [
      "text_chunk_embedding"
    ]
  },
  "query": {
    "neural": {
      "text_chunk_embedding": {
        "query_text": q,
        "model_id": config.MODEL_ID,,
        "k": 5
      }
    }
  }
}
```

OK, that's enough. We learned a lot, but his is getting out of hand, we are trying to do too many things. Let's start a new branch and work on a semantic search only version of this focusing on modularity of the ingest pipeline so that we can generalize it to other data sources in the future.
