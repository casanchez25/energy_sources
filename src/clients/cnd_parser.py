import os
import pandas as pd
from datetime import datetime, timedelta
import re
import zipfile

class CND_PARSER:

    def __init__(
      self,
      file_metadata:list,
      file_path:str,
      staging_path: str
    )->None:
        '''
        Parameters:
        - file_path (str): Path of the file for parsing
        - file_metadata (dict): Metadata of the downloaded files from CND.
            - file_id (int): The ID of the file.
            - file_name (str): The name of the file (extracted from its path).
            - epoch_public_date (str): The file's public release date in epoch format.
        - staging_path (str): staging path where the downloaded files are located.
        '''
        self.file_path=file_path
        self.file_metadata=file_metadata
        self.staging_path=staging_path

    def _sheetname(self,requested_date:datetime) ->str:
      '''
      This function fix the weekday number based on CND's logic
      Friday =7, Saturday=1, Sunday=2, Monday=3, Tuesday=4, Wednesday=5, Thursday=6
      Parameters:
      - requested_date (datetime): The date for which the weekday needs to be determined.
      Returns:
      - int: The weekday number based on CND's logic.
      '''
      weekday=requested_date.weekday()
      if weekday <= 4:
        weekday+=3
      else:
        weekday-=4
      return weekday

    def _adding_metadata(self, df:pd.DataFrame)-> pd.DataFrame:
      '''
      This function adds required metadata to the dataframe.
      Parameters:
      - df: Parsed dataframe
      Returns:
      - df: Parsed dataframe with metadata added
      '''
      # Convert to datetime
      epoch=re.search(string=self.file_metadata[2], pattern='([0-9]+)')[0]
      df['file_id']=self.file_metadata[0]
      df['file_name']=self.file_metadata[1]
      df['epoch_public_date']=epoch
      return df

    def _unzipping(self,output_staging_path:str)->None:
      zip_ref = zipfile.ZipFile(self.file_path, 'r')
      zip_ref.extractall(f'{output_staging_path}')
      zip_ref.close()

    def _rename_columns(self, df:pd.DataFrame, hours_cols_bool:bool = False) -> pd.DataFrame:
      '''
      This function removes special characters, white spaces to '', - to _ and $ to USD
      ''' 
      keys = df.columns.to_list()
      cols = [
          x.translate(str.maketrans({
              "-": "_", 
              " ": "", 
              "(": "", 
              ")": "", 
              "[": "", 
              "]": "", 
              "$": "_USD"})).strip().lower()
          for x in keys
      ]

      if hours_cols_bool: ## This is to change 1_2 column to h1_2 or 0 to h0, pretty common in CNDs files
        cols=[f'h{x}' if x[0].isdigit() else x for x in cols]
      
      cleaned_columns = {k:v for (k,v) in zip(keys,cols)}
      
      return df.rename(columns=cleaned_columns)


    def parse_predispatch(
      self,
      requested_date:datetime,
      header:int,
      output_prefix:str
    )-> None:
      '''
      This function parses the predispatch file. It unzip file if necessary and parse
      based on sheetname. In addition, uses the function __sheetname that let us get
      the name with CND's logic. The parsed file is saved in the staging path.
      Parameters:
      - requested_date: Date of the file
      - header: Header of the dataframe
      - output_prefix: Prefix of the output file
      Returns:
      - None
      '''

      ## Unzipping file if necessary
      if self.file_path.endswith('.zip'):
        staging_path_unzip=f'{self.staging_path}/UNZIP'

        try:
          self._unzipping(staging_path_unzip)
        except Exception as e:
          raise Exception(f'Error occurred while unzipping {self.file_path}: {e}')

        flist=os.listdir(staging_path_unzip)
        if flist==[]:
          raise Exception(f'Error occurred while unzipping {self.file_path}: {e}')
        else:
          flist = [f'{staging_path_unzip}/{f}' for f in flist]
          self.file_path=flist[0]

      # Load the Excel file
      try:
        excel_file = pd.ExcelFile(self.file_path)
      except Exception as e:
        raise Exception(f'Error occurred while loading {self.file_path}: {e}')

      # Get the sheet names
      sheet_names = excel_file.sheet_names
      print(f'Available sheets {sheet_names}')
      sheet_day=str(self._sheetname(requested_date))
      print(f'Getting {sheet_day}')
      sheet=[s for s in sheet_names if re.search(string=s, pattern=sheet_day)]

      ## Parsing Date fecha
      df = pd.read_excel(self.file_path,sheet_name=sheet[0])
      cols=df.columns.to_list()
      parsed_date=df[cols[0]][0]


      try:
        df=pd.read_excel(self.file_path,
                         header=header,
                         sheet_name=sheet[0])
      except Exception as e:
        raise Exception(f'Error occurred while reading {self.file_path}: {e}')

      df['fecha'] = parsed_date ## Adding parsed date
      df=self._adding_metadata(df)
      filename=self.file_metadata[1].split('.')[0]

      ## Removing special characters on columns and setting them in lower case
      df = self._rename_columns(df=df,hours_cols_bool=True)
      #df.to_csv(f'{self.staging_path}/{output_prefix}_{filename.lower()}.csv.gz',
      #          index=False,
      #          compression='gzip')
      df.to_parquet(f'{self.staging_path}/{output_prefix}_{filename.lower()}.parquet.gz',
                index=False,
                compression='gzip')

