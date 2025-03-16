import os
from datetime import datetime
from src.clients.cnd_downloader import CND_DOWNLOADER
from src.clients.cnd_parser import CND_PARSER
from src.clients.gcp_client import GCP_UPLOADER
import re
import shutil

def download_and_parse_files(
    requested_date:datetime,
    base_url:str,
    payload:dict,
    staging_path:str,
) -> None:
           

    cnd_client=CND_DOWNLOADER(
        requested_date=requested_date,
        base_url=base_url,
        payload=payload,
        staging_path=staging_path
    )
    file_metadata=cnd_client.cnd_file_download()
    downloaded_files=os.listdir(staging_path)

    ### Getting files to parse. CND can share zip, xls, xlsx
    files_to_parse = [
        f'{staging_path}/{f}' for f in downloaded_files 
        if re.search(string=f, pattern='.xl') or re.search(string=f, pattern='.zip') 
    ]
    print(f'Files to parse {files_to_parse}')

    ### Due to this task will be used for historical backfill, we will remove parsed files 
    files_to_remove = [
        f'{staging_path}/{f}' for f in downloaded_files 
        if not re.search(string=f, pattern='.gz')
    ]

    print(f'Files to remove {files_to_remove}')

    ## Parsing files
    for i in range(len(files_to_parse)):
        parser=CND_PARSER(
            file_path=files_to_parse[i],
            file_metadata=file_metadata[i],
            staging_path=staging_path
        )
        parser.parse_predispatch(requested_date,3,'cnd_predespacho_diario')

    ## Removing non required files. This is helpful for other iterations
    for f in files_to_remove:
        if os.path.isfile(f):
            os.remove(f)
        elif os.path.isdir(f):
            shutil.rmtree(f)
    
