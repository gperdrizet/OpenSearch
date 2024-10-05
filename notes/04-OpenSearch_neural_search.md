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

Response:

```text
{
  "task_id": "nqFnWpIBgWKaVuCy3PgJ",
  "status": "CREATED"
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

Now we need to start adapting this to our wikipedia parse/insert code. This whole thing will need a refactor to make it modular. The ideal case would be to completely separate all of the OpenSearch stuff from the Wikipedia specific stuff, that way it's reusable for other text sources.

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
          "text": "passage_embedding"
        }
      }
    }
  ]
}
```

Let's do that as part of index initialization (helper_funcs.initialize_index()). We also need to change how we are initializing the index. The tutorial gives the following:

```text
PUT /my-nlp-index
{
  "settings": {
    "index.knn": true,
    "default_pipeline": "nlp-ingest-pipeline"
  },
  "mappings": {
    "properties": {
      "id": {
        "type": "text"
      },
      "passage_embedding": {
        "type": "knn_vector",
        "dimension": 768,
        "method": {
          "engine": "lucene",
          "space_type": "l2",
          "name": "hnsw",
          "parameters": {}
        }
      },
      "text": {
        "type": "text"
      }
    }
  }
}
```
