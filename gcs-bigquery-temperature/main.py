from google.cloud import bigquery

def temperature_to_bigquery_func(data, context):
    # check content-type
    if data['contentType'] != 'text/csv':
        print('Not supported file type: {}'.format(data['contentType']))
        return
    # get file info
    bucket_name = data['bucket']
    file_name = data['name']
    uri = 'gs://{}/{}'.format(bucket_name, file_name)

    dataset_id = 'fro_gcs_test'
    table_id = 'fro_gcs_temperature'
    bq = bigquery.Client()
    dataset_ref = bq.dataset(dataset_id)

    # Set Load Config
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = 'WRITE_APPEND'

    # Load data
    load_job = bq.load_table_from_uri(
        uri, dataset_ref.table(table_id), job_config=job_config
    )
    print("Starting job {}".format(load_job.job_id))
    load_job.result()
    print("Job finished.")