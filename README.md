# Overview 
[!overview](images/overview.JPG)

### Monitoring

```
python monitoring.py --project gcp-expert-sandbox-allen --bucket gcp-expert-sandbox-allen-monitoring --dataset monitoring --table monitoring --filters "storage.googleapis.com/storage/object_count,storage.googleapis.com/storage/total_byte_seconds,storage.googleapis.com/storage/total_bytesbyte_count,storage.googleapis.com/storage/object_count"
```

### Cloud Scheduler Message-Body

Example:

```
{
"project":"gcp-expert-sandbox-allen",
"bucket":"gcp-expert-sandbox-allen-monitoring",
"dataset":"monitoring",
"table":"monitoring",
"filters":"storage.googleapis.com/storage/object_count,storage.googleapis.com/storage/total_byte_seconds,storage.googleapis.com/storage/total_bytes"
}
```
### Cloud Build Trigger Substitutions

Example:

```
_PROJECT_ID: $(body.message.data.project)
_GCS_BUCKET: $(body.message.data.bucket)
_BQ_DATASET: $(body.message.data.dataset)
_BQ_TABLE: $(body.message.data.table)
_METRIC_FILTERS: $(body.message.data.filters)
```

### Cloud Build YAML Substitutions

Var |Type |Example |Description
:---|:---|:---|:---
_IMAGE |String |us-central1-docker.pkg.dev/gcp-expert-sandbox-allen/demo/monitoring:v1 |Container Image
_TIMEZONE |String |Asia/Taipei |Time Zone
_PROJECT_ID |String |gcp-expert-sandbox-allen |Project Name
_BQ_DATASET |String |monitoring |BigQuery DataSet Name
_BQ_TABLE |String |monitoring |BigQuery Table Name
_GCS_BUCKET |String |gcp-expert-sandbox-allen-monitoring |GCS Bucket Name
_METRIC_FILTERS |String[] |storage.googleapis.com/storage/object_count,storage.googleapis.com/storage/total_byte_seconds,storage.googleapis.com/storage/total_bytes |Metric Filters