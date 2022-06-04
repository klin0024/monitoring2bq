#import logging
from google.cloud import monitoring_v3
from google.cloud import bigquery
from datetime import timedelta
from datetime import datetime
import time
import os
import json
import re
import argparse
from google.cloud.storage import Blob
from google.cloud import storage

monitoring_client = monitoring_v3.MetricServiceClient()
bq_client = bigquery.Client()
storage_client = storage.Client()

export_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def get_interval(weeks_ago=0, days_ago=0, hours_ago=0, seconds_ago=0):
    time_now = time.time()
    start_time = get_second_delta(weeks_ago, days_ago, hours_ago, seconds_ago)
    seconds = int(time_now)
    nanos = int((time_now - seconds) * 10 ** 9)

    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - int(start_time)), "nanos": nanos},
        }
    )

    return interval


def get_request_body(project_id, metric_filter, interval, page_size, full_view, seconds, per_series_aligner):
    project_name = f"projects/{project_id}"

    if full_view:
        view = monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
    else:
        view = monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.HEADERS
    
    aggregation = monitoring_v3.Aggregation(
        {
            "alignment_period": {"seconds": seconds},
            "per_series_aligner": per_series_aligner
        }
    )

    request = {
        "name": project_name,
        "filter": metric_filter,
        "interval": interval,
        "aggregation": aggregation,
        "view": view,
        "page_size": page_size,
    }
    return request


def get_metric_data(request):
    print("Sending API request to the monitoring backend")

    results = monitoring_client.list_time_series(request)

    print("Got response from the server")

    return results


def get_second_delta(weeks, days, hours, seconds):
    return timedelta(weeks=weeks, days=days, hours=hours, seconds=seconds).total_seconds()


def parse_as_json_new_line(data):
    print("Parsing data into data points")
    points = []
    for page in data:

        metric_name = page.metric
        resource_name = page.resource
        metric_kind = page.metric_kind
        value_type = page.value_type

        for point in page.points:

            dict_point = {
                'time': point.interval.start_time.strftime('%d/%m/%Y %H:%M:%S'),
                'metric_type': metric_name.type,
                'resource_type': resource_name.type,
                'metric_kind': metric_kind,
                'value_type': value_type,
                'int_value': point.value.int64_value,
                'double_value': point.value.double_value,
                'string_value': point.value.string_value,
                'bool_value': point.value.bool_value
            }
            
            #for key, value in metric_name.labels.items():
            #    dict_point[key] = value

            #for key, value in resource_name.labels.items():
            #    dict_point[key] = value

            metric_labels = {}
            for key, value in metric_name.labels.items():
                metric_labels[key] = value
            dict_point["metric_labels"] = json.dumps(metric_labels)

            resource_labels = {}
            for key, value in resource_name.labels.items():
                resource_labels[key] = value
            dict_point["resource_labels"] = json.dumps(resource_labels) 

            points.append(dict_point)

    print("Parsing completed")

    return points


def load_to_bq(project_id, dataset, table_name, gcs_path):
    print(f"BigQuery load destination: {project_id}:{dataset}.{table_name}")

    table_id = f"{project_id}.{dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True, schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )

    job = bq_client.load_table_from_uri(f'{gcs_path}', table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = bq_client.get_table(table_id)  # Make an API request.
    print(
        "Load job completed, total rows number is {} and with {} columns on {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def write_to_local_disk(file_prefix, page_number, page_data):
    page_local_path = f'/tmp/{re.sub("/","_",file_prefix)}_{page_number}.jsonl'
    print(f"Writing data point into local path {page_local_path}")
    with open(page_local_path, 'w') as out_file:
        for data_point in page_data:
            out_file.write(json.dumps(data_point))
            out_file.write("\n")

    print(f"Writing tmp file {page_local_path} completed with no errors")

    return page_local_path


def delete_local_file(path):
    print(f'Removing tmp local file {path}')
    os.remove(path)
    print(f'Local file {path} successfully removed')


def write_to_gcs(bucket_name, file_prefix, page_number, page_data):
    local_page_path = write_to_local_disk(file_prefix, page_number, page_data)
    bucket_path = storage_client.get_bucket(bucket_name)
    gcs_file_path = f'{file_prefix}/{export_datetime}/{page_number}.jsonl'

    print(f'Uploading local file {local_page_path} to gcs path {gcs_file_path}')

    blob = Blob(gcs_file_path, bucket_path)
    blob.upload_from_filename(local_page_path)

    print(f'Uploading {local_page_path} to gcs path {gcs_file_path} successfully done')

    delete_local_file(local_page_path)


def get_parsed_request(request):
    print(f'Request content:{request}')

    int_key_names = ['seconds',
                     'page_size']

    parsed_request = request.get_json()

    for name in int_key_names:
        parsed_request[name] = int(parsed_request[name])

    return parsed_request


def export(request):
    #parsed_request = get_parsed_request(request)
    parsed_request = request

    interval = get_interval(seconds_ago=parsed_request['seconds'])

    api_request = get_request_body(parsed_request['project_id'],
                                   parsed_request['filter'],
                                   interval,
                                   parsed_request['page_size'],
                                   True,
                                   parsed_request['seconds'],
                                   monitoring_v3.Aggregation.Aligner.ALIGN_MEAN)

    raw_metrics_data = get_metric_data(api_request)

    file_prefix = parsed_request['bq_destination_table'] + '/' + re.search(r'"(.*)"',parsed_request['filter']).group(1)
    page_num = 1
    parsed_page = parse_as_json_new_line(raw_metrics_data.time_series)
    write_to_gcs(parsed_request['bucket_name'],
                 file_prefix,
                 page_num,
                 parsed_page)

    while raw_metrics_data.next_page_token:
        api_request.update({
            'page_token': raw_metrics_data.next_page_token
        })
        raw_metrics_data = get_metric_data(api_request)

        page_num += 1

        parsed_page = parse_as_json_new_line(raw_metrics_data.time_series)
        write_to_gcs(parsed_request['bucket_name'],
                     file_prefix,
                     page_num,
                     parsed_page)

    gcs_path = f"gs://{parsed_request['bucket_name']}/{file_prefix}/{export_datetime}/*"
    
    load_to_bq(parsed_request['project_id'],
               parsed_request['bq_destination_dataset'],
               parsed_request['bq_destination_table'],
               gcs_path)

def main(argv=None):
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--project',
                        dest='project_id',
                        required=True)

    parser.add_argument('--seconds',
                        dest='seconds',
                        default=3600)

    parser.add_argument('--dataset',
                        dest='bq_destination_dataset',
                        required=True)

    parser.add_argument('--table',
                        dest='bq_destination_table',
                        required=True)

    parser.add_argument('--size',
                        dest='page_size',
                        default=500)

    parser.add_argument('--bucket',
                        dest='bucket_name',
                        required=True)

    parser.add_argument('--filters',
                        dest='filters',
                        required=True)

    known_args, _ = parser.parse_known_args(argv)

    request = {}

    filters = known_args.filters.split(',')

    request['project_id'] = known_args.project_id
    request['seconds'] = int(known_args.seconds)
    request['bq_destination_dataset'] = known_args.bq_destination_dataset
    request['bq_destination_table'] = known_args.bq_destination_table
    request['page_size'] = int(known_args.page_size)
    request['bucket_name'] = known_args.bucket_name

    
    for filter in filters:
        request['filter'] = f'metric.type = "{filter}"'
        export(request)

if __name__ == '__main__':
    main()    