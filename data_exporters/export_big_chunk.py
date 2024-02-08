import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/ultimate-aspect-410714-42e5b7775bcc.json"

bucket_name = 'mage_zoomcamp-etido-1'
project_id = 'ultimate-aspect-410714'

table_name = "nyc_yellow_taxi_data"

root_path = f'{bucket_name}/{table_name}.parquet'

@data_exporter
def export_data(data, *args, **kwargs):
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    with gcs.open_output_stream(root_path) as out:
        pq.write_table(table, out)
