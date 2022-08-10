'''
Creator : Azid
Objective: This is image to use for any pipeline that pull parquet files from GCS and upload to BigQuery

Flag:
    --n-jobs 1
    --bucket-name gs://yourbucketname
    --prefix path/to/your/table/
    --st 2022-01-01
    --ed 2022-12-31
    --dt-format %Y-%m-%d
    --project-id yourprojectid
    --dataset-id yourdatasetid
    --table-id yourtableid
    --location US
    --temp-table false
    --auto-schema false
    --write-disposition WRITE_APPEND
    --time-partitioning '{}'
    --sa your-sa-key.json
    --topic yourtopic

=======================================

time python3 ./main.py \
--n-jobs 8 \
--bucket-name gs://yourbucketname \
--prefix path/to/your/table/ \
--st 2021-01-01 \
--ed 2022-12-31 \
--dt-format %Y-%m-%d \
--project-id yourprojectid \
--dataset-id yourdatasetid \
--table-id yourtableid \
--location US \
--temp-table false \
--auto-schema true \
--write-disposition WRITE_APPEND \
--time-partitioning '{}' \
--sa your-sa-key.json \
--topic yourtopic

'''

from lib.classes import ParquetGCS
import argparse
import os

def main():
    parser = argparse.ArgumentParser(
        description='Script to pull parquet files from GCS and upload to BigQuery.'
    )
    parser.add_argument('--n-jobs', type=str, default='1')
    parser.add_argument('--bucket-name', type=str, required=True)
    parser.add_argument('--prefix', type=str, required=True)
    parser.add_argument('--st', type=str, required=True)
    parser.add_argument('--ed', type=str, required=True)
    parser.add_argument('--dt-format', type=str, required=True)
    parser.add_argument('--project-id', type=str, required=True)
    parser.add_argument('--dataset-id', type=str, required=True)
    parser.add_argument('--table-id', type=str, required=True)
    parser.add_argument('--location', type=str, required=True)
    parser.add_argument('--temp-table', type=str, default="false")
    parser.add_argument('--auto-schema', type=str, default="false")
    parser.add_argument('--write-disposition', type=str, default="WRITE_APPEND")
    parser.add_argument('--time-partitioning', type=str, default="{}")
    parser.add_argument('--sa', type=str, required=True)
    parser.add_argument('--topic', type=str, required=True)
    args = parser.parse_args()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f'/secret1/{args.sa}'
    os.environ["CLOUDSDK_PYTHON"] = "python3"

    pipeline = ParquetGCS(
        n_jobs=args.n_jobs,
        bucket_name=args.bucket_name,
        prefix=args.prefix,
        st=args.st,
        ed=args.ed,
        dt_format=args.dt_format,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id,
        location=args.location,
        temp_table=args.temp_table,
        auto_schema=args.auto_schema,
        time_partitioning=args.time_partitioning,
        write_disposition=args.write_disposition,
        topic=args.topic,
    )
    pipeline.fetch_data()
    pipeline.fetch_schema()
    pipeline.upload_to_bq()

if __name__ == '__main__':
    main()