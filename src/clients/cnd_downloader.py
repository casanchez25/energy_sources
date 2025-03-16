import requests
import os
from datetime import datetime


class CND_DOWNLOADER:

    def __init__(
        self,
        requested_date:datetime,
        base_url:str,
        payload: dict,
        staging_path: str,
        week_bool:bool=False,
    )->None:
        '''
        Parameters:
        - requested_date (datetime): Reference date for downloading file
        - base_url (str): Base url from CND to download files
        - payload (dict): A dictionary containing query parameters for the request.
        - staging_path (str): The directory path where the downloaded files will be stored.
        - week_bool (bool): Boolean that defines if it is a weekly or daily file
        '''
        self.requested_date=requested_date
        self.base_url=base_url
        self.payload=payload
        self.staging_path=staging_path
        self.week_bool=week_bool

    def _adjust_header_date(self) -> dict:
      '''
      This function adds the date parameters for the query parameters of the request
      Parameters:
      - None
      Returns:
      - dict: A dictionary containing the updated query parameters.
      '''

      self.payload['anio']=str(self.requested_date.year)
      if self.week_bool:
        self.payload['semana']=str(self.requested_date.isocalendar()[1])
        self.payload['dia']='0'
        self.payload['mes']='0'
      else:
        self.payload['semana']='0'
        self.payload['dia']=str(self.requested_date.day)
        self.payload['mes']=str(self.requested_date.month)
      return self.payload

    def cnd_file_download(self) -> list:
        '''
        Downloads files from the CND site based on the provided parameters.

        Parameters:
        - self: From class initialization

        Returns:
        - list: A list of tuples, where each tuple contains:
            - file_id (int): The ID of the file.
            - file_name (str): The name of the file (extracted from its path).
            - epoch_public_date (str): The file's public release date in epoch format.

        Raises:
        - Exception: If the metadata request or file download fails.
        '''
        self.payload=self._adjust_header_date()

        try:
            # Send the GET request to the API to retrieve metadata
            response = requests.get(self.base_url, params=self.payload)

            # Check for a successful response
            if response.status_code != 200 or response.json()==[]:
                raise Exception(f'Failed to retrieve metadata. HTTP Status Code: {response.status_code}, Message: {response.text}')

            # Extract file metadata from the JSON response
            file_metadata = [
                (r['id'],
                 os.path.basename(r['adjunto']['path']).split('\\')[-1],
                 r['fechaPublica']
                 )
                for r in response.json()
            ]
        except requests.exceptions.RequestException as e:
            raise Exception(f'Request error occurred while retrieving metadata: {e}')
        except Exception as e:
            raise Exception(f'Error occurred while fetching metadata: {e}')

        # If the metadata request succeeded, proceed to download the files
        download_url = 'https://sitioprivado.cnd.com.pa/Informe/Download'

        # Download each file
        for file_id, file_name, _ in file_metadata:
            try:
                file_payload = {'key': self.payload['key']}
                file_response = requests.get(f'{download_url}/{file_id}', params=file_payload)

                # Check if file download was successful
                if file_response.status_code != 200:
                    raise Exception(f'Error downloading file {file_name}. HTTP Status Code: {file_response.status_code}')

                # Ensure the storage path exists
                os.makedirs(self.staging_path, exist_ok=True)

                # Save the file to disk
                with open(f'{self.staging_path}/{file_name}', 'wb') as f:
                    f.write(file_response.content)

                print(f'Downloaded {file_name} to {self.staging_path}')

            except requests.exceptions.RequestException as e:
                raise Exception(f'Error occurred while downloading {file_name}: {e}')

        return file_metadata
