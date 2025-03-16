import os
from datetime import datetime
from src.clients.cnd_downloader import CND_DOWNLOADER
from src.clients.cnd_parser import CND_PARSER
from src.clients.gcp_client import GCP_UPLOADER
from src.clients.date_adjuster import DATE_ADJUSTER
from src.clients.remove_directory import rm_directory
from src.pipeline.tasks.cnd_predispatch_downparse import download_and_parse_files
import time
import shutil

def cnd_predispatch(
    base_url:str,
    payload:dict,
    staging_path:str,
    cred_path:str=None,
    gcp_project_id:str=None,
    bucket_name:str=None,
    blob_folder_name:str=None,
    gcs_regexp_file:str=None,
    requested_date:datetime =None,
    days_shift:int =0,
    days_backfill:int=0
) -> None:
    
    # Removing temp files if we set it for loading to GCP
    if gcp_project_id != None or bucket_name != None:
        ## Removing Temp folder
        rm_directory(staging_path)
        
    ## For loop for historical backfill
    for d in range(days_backfill):
        ## Fixing date requirement
        days_shift += 1
        
        adjuster=DATE_ADJUSTER(input_date=requested_date, shift_days=days_shift*-1)
        requested_date_adjusted=adjuster.adjust_date()
        print(requested_date_adjusted)

        date_str =requested_date_adjusted.strftime("%Y-%m-%d")
        print(f'Downloading for day {date_str}')
        
        try:
            ## Downloading and parse data task
            download_and_parse_files(
                requested_date=requested_date_adjusted,
                base_url=base_url,
                payload=payload,
                staging_path=staging_path
            )

            rm_directory(f'{staging_path}/UNZIP')
        except:
            print(f'Issue parsing files from {date_str}')
            os.makedirs('archive', exist_ok=True)
            dest = shutil.move(staging_path, f'archive_{date_str}') 
        
        # Load to GCP 
        if gcp_project_id != None or bucket_name != None: ## One of them is None, we will only save them local
            gcp_client=GCP_UPLOADER(
                gcp_project_id=gcp_project_id,
                staging_path=staging_path,
                cred_path=cred_path
            )

            gcs_folder_name= f'{blob_folder_name}/{date_str}'

            gcp_client.gcs_upload(
                    bucket_name=bucket_name,
                    blob_folder_name=gcs_folder_name,
                    regexp_file=gcs_regexp_file
            )
            
            rm_directory(staging_path)

        ## Delay time to avoid being blacklisted
        time.sleep(3)
        

