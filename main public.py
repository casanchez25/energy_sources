from src.prefect.flows.cnd_predispatch_flow import cnd_predispatch
import shutil
import os
from datetime import datetime



if __name__ == '__main__':
        ## Parameters
        doc_num='76'
        category='6'
        page='0'
        key='public_key'

        payload={
                'categoria': category,
                'tipo':doc_num,
                'key': key,
                'page': page,
                'publico':'1'
        }
        staging_path='temp'
        base_url = 'https://sitioprivado.cnd.com.pa/Informe/GetListOperativosComerciales'

        requested_date=datetime(2025,3,6)
        days_shift=0
        days_backfill=5

        gcp_project_id='terst-project'
        bucket_name='test_bucket'
        blob_folder_name='blob_folder'
        gcs_regexp_file='.parquet.gz'
        cred_path ='credentials_path'
        

        cnd_predispatch(
                base_url=base_url,
                payload=payload,
                staging_path=staging_path,
                cred_path=cred_path,
                gcp_project_id=gcp_project_id,
                bucket_name=bucket_name,
                blob_folder_name=blob_folder_name,
                gcs_regexp_file=gcs_regexp_file,
                requested_date=requested_date,
                days_shift=days_shift,
                days_backfill=days_backfill
        )