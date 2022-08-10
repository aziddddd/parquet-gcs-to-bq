from joblib import Parallel, delayed
import multiprocessing as mp
from tqdm import tqdm

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import pandas as pd
import itertools
import json

import pyarrow.parquet as pq
import gcsfs

class ParquetGCSError(Exception):
    """ An exception class for ParquetGCS """
    pass

class ParquetGCS(object):
    """ A class to pull parquet files from GCS and upload to BigQuery """
    def __init__(
        self,
        n_jobs: str=1,
        bucket_name: str=None,
        prefix: str=None,
        st: str=None,
        ed: str=None,
        dt_format: str='%Y-%m-%d',
        project_id: str=None,
        dataset_id: str=None,
        table_id: str=None,
        location: str=None,
        temp_table: str='false', # true/false
        auto_schema: str='false', # true/false
        time_partitioning: str=None,
        write_disposition: str=None,
        topic: str='topic1',
    ):
        if isinstance(time_partitioning, str):
            time_partitioning_dict = json.loads(time_partitioning)
        else:
            raise ParquetGCSError('Provided time_partitioning is not JSON serializable.')
        self.time_partitioning = time_partitioning_dict

        self.auto_schema = auto_schema
        self.write_disposition = write_disposition
        self.topic = topic

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.st = st
        self.ed = ed
        self.dt_format = dt_format

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.location = location
        self.temp_table = temp_table

        self.check_essential_resources()
        self.upload_flag = True
        self.n_jobs = int(n_jobs)

    def check_essential_resources(self):
        """ Check if the outbound dataset is exist, if not exist then create one """
        client = bigquery.Client(
            project=self.project_id,
            location=self.location,
        )
        datasets = [i.dataset_id for i in client.list_datasets()]
        full_dataset_id = f"{self.project_id}.{self.dataset_id}"

        # if dataset not exist, then create new one.
        if self.dataset_id not in datasets:
            print(f"Dataset '{self.dataset_id}' is not exists, creating a new one..")
            dataset = bigquery.Dataset(full_dataset_id)
            dataset.location = self.location
            dataset = client.create_dataset(dataset, timeout=30)
            print("Created dataset, dataset_id :", full_dataset_id)
        else:
            print("Dataset exists, dataset_id :", full_dataset_id)

    def fetch_data(self):
        storage_client = storage.Client(project=self.project_id)

        st_obj = datetime.strptime(self.st, self.dt_format)
        ed_obj = datetime.strptime(self.ed, self.dt_format)
        dates = [i.strftime(self.dt_format) for i in pd.date_range(st_obj, ed_obj, freq='d').tolist()]
        self.gcs_uri = []
        columns = []

        for step_dt in dates:
            print(f'Pulling new records for dt={step_dt}.')
            remote_dir = f'{self.prefix}/dt={step_dt}/'

            blobs = storage_client.list_blobs(
                self.bucket_name.replace('gs://', ''),
                prefix=remote_dir,
            )
            parquet_files = [f'{self.bucket_name}/{blob.name}' for blob in blobs if blob.name.endswith('.gz.parquet')]

            if parquet_files:
                self.gcs_uri.extend(parquet_files)
                if self.auto_schema == 'true':
                    step_columns = Parallel(n_jobs=self.n_jobs)(delayed(self.parallelize_fetch)(filename) for filename in tqdm(parquet_files))
                    columns.extend(list(itertools.chain(*step_columns)))
            else:
                print(f'No new records to pull for dt={step_dt}.')

        self.columns = list(set(columns))

        if self.gcs_uri:
            print(f'{len(self.gcs_uri)} parquet files in gcs to upload.')
        else:
            print(f'No parquet files in gcs to upload.')
            self.upload_flag = False

    def parallelize_fetch(self, filename):
        fs = gcsfs.GCSFileSystem()
        f = fs.open(filename)
        schema = pq.ParquetFile(f).schema
        return schema.names

    def fetch_schema(self):
        storage_client = storage.Client(project=self.project_id)
        bucket = storage_client.get_bucket(self.bucket_name.replace('gs://', ''))

        local_dir = f'/tmp/{self.topic}__{self.table_id}.json'
        remote_dir = f'schema/{self.topic}/{self.table_id}/'

        blobs = storage_client.list_blobs(
            self.bucket_name.replace('gs://', ''),
            prefix=remote_dir,
        )

        # check existing schema_mapping
        check = False
        latest_schema = []
        for blob in blobs:
            latest_schema.append(blob.name)
            check=True

        print(f'Check for {remote_dir} : {check}')
        # use existing schema_mapping, update if there's new key
        if check:
            print(f'Use existing schema mapping...')
            print(f'latest_schema: {latest_schema[-1]}')
            blob = bucket.blob(latest_schema[-1])
            blob.download_to_filename(local_dir)
            with open(local_dir, 'r') as openfile:
                schema_mapping = json.load(openfile)
            for val in self.columns:
                if str(val) not in schema_mapping:
                    schema_mapping[str(val)] = 'STRING'

        # create new schema_mapping
        else:
            print(f'Create a new schema mapping...')
            schema_mapping = {str(i): 'STRING' for (idx, i) in enumerate(self.columns)}

        update = True
        # check any changes in schema mapping
        if len(latest_schema) > 1:
            blob = bucket.blob(latest_schema[-2])
            blob.download_to_filename(local_dir)
            with open(local_dir, 'r') as openfile:
                old_schema_mapping = json.load(openfile)
            
            if old_schema_mapping.keys() == schema_mapping.keys():
                update = False

        # update schema mapping if there's any changes
        if update:
            src_run_dt = datetime.strftime(datetime.now(), "%Y-%m-%dT%H_%M_%S")
            schema_filename = f'{self.table_id}_{src_run_dt}.json'
            print(f"Schema mapping changes, uploading new schema : {schema_filename}")

            schema_mapping_str = json.dumps(schema_mapping)
            with open(local_dir, 'w') as outfile:
                outfile.write(schema_mapping_str)
            blob = bucket.blob(remote_dir + schema_filename)
            blob.upload_from_filename(local_dir)
        else:
            print(f"Schema mapping remains the same...")

        self.schema_obj = [
            {
                "mode": "NULLABLE",
                "name": name,
                "field_type": field_type
            } for (name, field_type) in schema_mapping.items()
        ]

        print(f'Current Schema : {self.schema_obj}')

    def chunks(self, lst, n):
        """ Yield successive n-sized chunks from lst. """
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def upload_to_bq(self):
        """ Upload processed data to BigQuery """
        try:
            # Upload to BQ
            if not self.upload_flag:
                print(f'No new records to upload.')
                return

            if self.temp_table == 'true':
                self.dataset_id = 'TEMP'
                table_name = f'{self.table_id}_STG'
                print(f'Saving to temporary BQ path... : {self.project_id}.{self.dataset_id}.{table_name}')
            elif self.temp_table == 'false':
                table_name = self.table_id
            else:
                raise ParquetGCSError('Invalid temp_table is provided.')

            client = bigquery.Client(
                project=self.project_id,
                location=self.location,
            )

            table = f'{self.project_id}.{self.dataset_id}.{table_name}'

            if self.auto_schema == 'true':
                schema_obj = self.schema_obj
            elif self.auto_schema == 'false':
                with open(f'schema/{self.topic}.json', 'r') as openfile:
                    schema_obj = json.load(openfile)
            else:
                raise ParquetGCSError('Invalid auto_schema is provided.')

            job_config_params ={
                'source_format': 'PARQUET',
                'schema' : [
                    bigquery.SchemaField( # non-nested field
                        name=i.get('name',''),
                        field_type=i.get('field_type',''),
                        mode=i.get('mode',''),
                        fields=()
                    ) if 'fields' not in i else \
                    bigquery.SchemaField( # nested field
                        name=i.get('name',''),
                        field_type=i.get('field_type',''),
                        mode=i.get('mode',''),
                        fields=[bigquery.SchemaField(**field_details) for field_details in i.get('fields','')],
                    ) \
                    for i in schema_obj
                ],
                'create_disposition': bigquery.CreateDisposition.CREATE_IF_NEEDED
            }

            if self.write_disposition == 'WRITE_TRUNCATE':
                _write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            elif self.write_disposition == 'WRITE_APPEND':
                _write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                raise ParquetGCSError('Invalid WriteDisposition method is provided.')

            job_config_params['write_disposition'] = _write_disposition

            if self.time_partitioning:
                _time_partitioning = bigquery.table.TimePartitioning(**self.time_partitioning)
                job_config_params['time_partitioning'] = _time_partitioning

            job_config = bigquery.LoadJobConfig(**job_config_params)

            gcs_uri_batches = list(self.chunks(self.gcs_uri, 10000))
            for idx, uri_batch in enumerate(gcs_uri_batches):
                print(f'Uploading batch {idx+1}/{len(gcs_uri_batches)} to {table}.')
                load_job = client.load_table_from_uri(
                    source_uris=uri_batch,
                    destination=table,
                    project=self.project_id,
                    location=self.location,
                    job_config=job_config,
                ) # Make an API request.

            load_job.result() # Waits for the job to complete.
            print(f'Data from {self.st} to {self.ed} are uploaded to {table}.')
            print(f'Total gcs URI : {len(self.gcs_uri)}')

        except BadRequest as err:
            print("Retrieved google.api_core.exceptions.BadRequest error for empty data, skipping the ingestion...")
            print(f"Error Message : {err}")

        except Exception as err:
            raise ParquetGCSError(err)