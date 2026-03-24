"""
GCP Integration Utilities for AeroStream
Handles all GCS and BigQuery operations
"""

from google.cloud import storage, bigquery
from google.oauth2 import service_account
import os
import logging
from datetime import datetime
from typing import Optional, List, Dict
import json

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class GCPClient:
    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initializes the GCPClient with credentials
        If no credentials are found, it will attempt to use Application Default Credentials
        """
        self.project_id = os.getenv('GCP_PROJECT_ID', 'aerostream-project')
        self.bucket_name = os.getenv('GCS_BUCKET_NAME', 'aerostream-data-lake')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'aviation')

        if credentials_path and os.path.exists(credentials_path):
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.storage_client = storage.Client(
                credentials=self.credentials,
                project=self.project_id
            )
            self.bigquery_client = bigquery.Client(
                credentials=self.credentials,
                project=self.project_id
            )
            log.info("✅ GCP Client initialized with service account credentials")
        else:
            self.storage_client = storage.Client(project=self.project_id)
            self.bigquery_client = bigquery.Client(project=self.project_id)
            log.info("✅ GCP Client initialized with Application Default Credentials")

    def upload_to_gcs(self, local_file_path: str, gcs_file_path: str) -> bool:
        """
        Uploads a file to Google Cloud Storage
        :param local_file_path: Path to the local file to upload
        :param gcs_file_path: Path in the GCS bucket where the file will be stored
        :return: True if upload is successful, False otherwise
        """
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(local_file_path)
            log.info(f"✅ Uploaded {local_file_path} to gs://{self.bucket_name}/{gcs_file_path}")
            return True
        except Exception as e:
            log.error(f"❌ Failed to upload {local_file_path} to GCS: {e}")
            return False
        
    def upload_directory_to_gcs(self, local_dir: str, gcs_prefix: str) -> bool:
        """
        Upload the entire contents of a local directory to GCS under a specified prefix
        :param local_dir: Path to the local directory to upload
        :param gcs_prefix: Prefix in the GCS bucket where the files will be stored
        :return: True if all uploads are successful, False otherwise
        """
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            for dirpath, dirnames, filenames in os.walk(local_dir):
                for filename in filenames:
                    local_file_path = os.path.join(dirpath, filename)
                    relative_path = os.path.relpath(local_file_path, local_dir)
                    gcs_file_path = os.path.join(gcs_prefix, relative_path)
                    blob = bucket.blob(gcs_file_path)
                    blob.upload_from_filename(local_file_path)
                    log.info(f"✅ Uploaded {local_file_path} to gs://{self.bucket_name}/{gcs_file_path}")

            return True
        except Exception as e:
            log.error(f"❌ Failed to upload directory {local_dir} to GCS: {e}")
            return False
    
    def list_gcs_files(self, prefix: str) -> List[str]:
        """
        Lists all files in the GCS bucket with the specified prefix
        :param prefix: Prefix to filter files in the GCS bucket
        :return: List of file paths in GCS that match the prefix
        """
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            file_list = [blob.name for blob in blobs]
            log.info(f"📋 Found {len(file_list)} files in gs://{self.bucket_name}/{prefix}")
            return file_list
        except Exception as e:
            log.error(f"❌ Failed to list files in GCS with prefix {prefix}: {e}")
            return []
        
    def create_dataset_if_not_exists(self) -> bool:
        """
        Creates the BigQuery dataset if it does not already exist
        """
        try:
            dataset_ref = self.bigquery_client.dataset(self.dataset_id)
            try:
                self.bigquery_client.get_dataset(dataset_ref)
                log.info(f"✅ BigQuery dataset '{self.dataset_id}' already exists")
            except Exception:
                # dataset does not exist, create it
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = os.getenv('GCP_REGION', 'US')
                dataset = self.bigquery_client.create_dataset(dataset)
                log.info(f"✅ Created BigQuery dataset '{self.dataset_id}'")
            
            return True
        except Exception as e:
            log.error(f"❌ Failed to create or access BigQuery dataset '{self.dataset_id}': {e}")
            return False

    def create_table_from_schema(
            self,
            table_id: str,
            schema: List[bigquery.SchemaField],
            partition_field: Optional[str] = None,
            clustering_fields: Optional[List[str]] = None
    ) -> bool:
        """
        Creates a BigQuery table with the specified schema, partitioning, and clustering
        :param table_id: Name of the table to create (without dataset prefix)
        :param schema: List of BigQuery.SchemaField objects defining the table schema
        :param partition_field: Optional field name to use for partitioning
        :param clustering_fields: Optional list of field names to use for clustering
        :return: True if table is created successfully, False otherwise
        """
        try:
            table_ref = self.bigquery_client.dataset(self.dataset_id).table(table_id)

            # check if table already exists
            try:
                self.bigquery_client.get_table(table_ref)
                log.info(f"✅ BigQuery table '{self.dataset_id}.{table_id}' already exists")
                return True
            except Exception:
                # table does not exist, we will create it
                table = bigquery.Table(table_ref, schema=schema)
                if partition_field:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=partition_field
                    )
                if clustering_fields:
                    table.clustering_fields = clustering_fields

                table = self.bigquery_client.create_table(table)
                log.info(f"✅ Created BigQuery table '{self.dataset_id}.{table_id}' with schema and partitioning/clustering")
                return True
        except Exception as e:
            log.error(f"❌ Failed to create BigQuery table '{self.dataset_id}.{table_id}': {e}")
            return False
        
    def load_parquet_from_gcs(
            self,
            gcs_uri: str,
            table_id: str,
            write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND
    ) -> bool:
        """
        Loads a Parquet file from GCS into a BigQuery table
        :param gcs_uri: URI of the Parquet file in GCS (e.g. gs://bucket/path/file.parquet)
        :param table_id: Name of the BigQuery table to load into (without dataset prefix)
        :param write_disposition: Write disposition for the load job (default is WRITE_APPEND)
        :return: True if load is successful, False otherwise
        """
        try:
            table_ref = self.bigquery_client.dataset(self.dataset_id).table(table_id)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
                autodetect=False  # We will rely on the table schema, not autodetection
            )

            load_job = self.bigquery_client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()  # Wait for the job to complete
            log.info(f"✅ Loaded data from {gcs_uri} into BigQuery table '{self.dataset_id}.{table_id}'")
            return True
        except Exception as e:
            log.error(f"❌ Failed to load data from {gcs_uri} into BigQuery: {e}")
            return False

    def run_query(self, query: str) -> Optional[bigquery.table.RowIterator]:
        """
        Runs a SQL query against BigQuery and returns the results as a list of dictionaries
        :param query: SQL query string to execute
        :return: List of dictionaries representing the query results
        """
        try:
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            # result_list = [dict(row) for row in results]
            log.info(f"✅ Successfully ran query and retrieved {len(results)} rows")
            return results
        except Exception as e:
            log.error(f"❌ Failed to run query: {e}")
            raise 
        
    def export_to_gcs(self, table_id: str, gcs_path, format: str = 'PARQUET') -> bool:
        """
        Exports a BigQuery table to GCS in the specified format
        :param table_id: Name of the BigQuery table to export (without dataset prefix)
        :param gcs_path: GCS path where the exported file will be stored (e.g. gs://bucket/path/file)
        :param format: Format to export in ('PARQUET', 'CSV', 'JSON', or 'AVRO')
        :return: True if export is successful, False otherwise
        """
        try:
            table_ref = self.bigquery_client.dataset(self.dataset_id).table(table_id)

            job_config = bigquery.ExtractJobConfig()
            if format.upper() == 'PARQUET':
                job_config.destination_format = bigquery.DestinationFormat.PARQUET
            elif format.upper() == 'CSV':
                job_config.destination_format = bigquery.DestinationFormat.CSV
                job_config.field_delimiter = ','
                job_config.print_header = True
            elif format.upper() == 'JSON':
                job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
            elif format.upper() == 'AVRO':
                job_config.destination_format = bigquery.DestinationFormat.AVRO
            else:
                log.error(f"❌ Unsupported export format: {format}")
                return False

            extract_job = self.bigquery_client.extract_table(
                table_ref,
                f"gs://{self.bucket_name}/{gcs_path}",
                job_config=job_config
            )
            extract_job.result()  # Wait for the job to complete
            log.info(f"✅ Exported BigQuery table '{self.dataset_id}.{table_id}' to gs://{self.bucket_name}/{gcs_path} in {format} format")
            return True
        except Exception as e:
            log.error(f"❌ Failed to export BigQuery table '{self.dataset_id}.{table_id}' to GCS: {e}")
            return False
        
# Predefined schemas for BigQuery tables
FLIGHT_RAW_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("icao24", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("callsign", "STRING"),
    bigquery.SchemaField("origin_country", "STRING"),
    bigquery.SchemaField("time_position", "INTEGER"),
    bigquery.SchemaField("last_contact", "INTEGER"),
    bigquery.SchemaField("longitude", "FLOAT"),
    bigquery.SchemaField("latitude", "FLOAT"),
    bigquery.SchemaField("baro_altitude", "FLOAT"),
    bigquery.SchemaField("on_ground", "BOOLEAN"),
    bigquery.SchemaField("velocity", "FLOAT"),
    bigquery.SchemaField("true_track", "FLOAT"),
    bigquery.SchemaField("vertical_rate", "FLOAT"),
    bigquery.SchemaField("geo_altitude", "FLOAT"),
    bigquery.SchemaField("ingestion_timestamp", "INTEGER"),
    bigquery.SchemaField("ingestion_date", "DATE"),
    bigquery.SchemaField("ingestion_hour", "STRING")
]

FLIGHT_METRICS_SCHEMA = [
    bigquery.SchemaField("icao24", "STRING"),
    bigquery.SchemaField("callsign", "STRING"),
    bigquery.SchemaField("origin_country", "STRING"),
    bigquery.SchemaField("altitude_km", "FLOAT"),
    bigquery.SchemaField("speed_kmh", "FLOAT"),
    bigquery.SchemaField("flight_phase", "STRING"),
    bigquery.SchemaField("is_anomaly", "BOOLEAN"),
    bigquery.SchemaField("ingestion_date", "DATE"),
    bigquery.SchemaField("processing_time", "TIMESTAMP")
]


if __name__ == "__main__":
    # Test the GCP client
    gcp_client = GCPClient()

    # Create dataset
    gcp_client.create_dataset_if_not_exists()

    # create tables
    gcp_client.create_table_from_schema(
        "raw_flights",
        FLIGHT_RAW_SCHEMA,
        partition_field="ingestion_date",
        clustering_fields=["origin_country", "icao24"]
    )