from google.cloud import storage, bigquery
from google.oauth2 import service_account
import os
import re

class GCP_UPLOADER:
    def __init__(
        self, 
        gcp_project_id: str, 
        staging_path: str, 
        cred_path:str = None
    ) -> None:
        '''
        Initialize GCP_UPLOADER with project ID and local staging path.
        Parameters:
        - gcp_project_id (str): GCP Project 
        - staging_path (str): Path with the files to upload
        '''
        self.gcp_project_id = gcp_project_id
        self.staging_path = staging_path
        if cred_path is not None:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS']= cred_path

    def gcs_upload(
        self,
        bucket_name: str,
        blob_folder_name: str,
        regexp_file: str = None
    ) -> None:
        '''
        Uploads files from local staging folder to GCS bucket.
        Optionally filters files based on regex pattern.
        Parameters:
        - bucket_name (str): name of the Google Cloud Storage bucket
        - blob_folder_name (str): Destination folder in Google Cloud Storage Bucket
        - regexp_file (str) Default None: Regexp for filtering the required files, for instance 'FILE[0-9]+.csv.gz'
        '''
        try:
            # Create GCS client
            storage_client = storage.Client(project=self.gcp_project_id)
        except Exception as e:
            print(f'[Error] Failed to create GCS client: {e}')
            return

        try:
            # Get bucket reference
            bucket = storage_client.bucket(bucket_name)
        except Exception as e:
            print(f'[Error] Failed to access bucket "{bucket_name}": {e}')
            return

        try:
            # Traverse local staging folder
            file_list = os.walk(self.staging_path)
            for root, dirs, files in file_list:
                for f in files:
                    try:
                        # Check regex match if provided
                        if regexp_file is not None and not re.search(pattern=regexp_file.lower(), string=f.lower()):
                            continue

                        # Build blob name and upload path
                        blob_name = os.path.join(blob_folder_name, f)
                        local_path = os.path.join(root, f)
                        
                        # Upload file to GCS
                        blob = bucket.blob(blob_name)
                        blob.upload_from_filename(local_path)
                        print(f'[Info] Uploaded {local_path} to {blob_name}')
                    except Exception as e:
                        print(f'[Error] Failed to upload {file}: {e}')
            
            storage_client.close()
        except Exception as e:
            print(f'[Error] Failed during file traversal/upload: {e}')

    def sp_bq_upload(
        self,
        bq_dataset: str,
        bq_table: str,
        sp_name: str,
        bucket_name: str,
        blob_folder_path: str,
        merge_key_columns: list
    ) -> None:
        '''
        Calls a BigQuery stored procedure to load/merge GCS data into a table.
        '''
        try:
            # Create BigQuery client
            os.environ['GOOGLE_APPLICATION_CREDENTIALS']= self.cred_path
            bq_client = bigquery.Client(project=self.gcp_project_id)
        except Exception as e:
            print(f'[Error] Failed to create BigQuery client: {e}')
            return

    
        # Build merge condition for stored procedure
        merge_conditions = [f'T.{col}=S.{col}' for col in merge_key_columns]
        merge_condition_str = ' AND '.join(merge_conditions)

        # Construct the stored procedure call query
        query = f'''
            CALL `{bq_dataset}.{sp_name}`(
                '{bucket_name}',
                '{blob_folder_path}',
                '{bq_table}',
                '{merge_condition_str}'
            )
        '''

        try:
            # Run the query
            query_job = bq_client.query(query)
            query_job.result()  # Wait for completion
            print(f'[Info] Stored procedure {sp_name} executed successfully.')
        except Exception as e:
            print(f'[Error] Failed to execute stored procedure: {e}')
