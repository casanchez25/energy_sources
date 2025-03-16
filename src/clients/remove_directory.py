import shutil
import os

def rm_directory(staging_path:str)->None:
    ## Removing Temp folder
    if os.path.exists(staging_path) and os.path.isdir(staging_path):
        shutil.rmtree(staging_path)
        print("Folder deleted.")
    else:
        print("Folder does not exist — skipping deletion.")