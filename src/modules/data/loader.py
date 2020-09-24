from os import path, listdir, getcwd

import csv
import requests
import sys
import zipfile

import pandas as pd

ZIP_REMOTE_PATH = 'https://stdatalake005.blob.core.windows.net/public/movies_dataset.zip'
ZIP_LOCAL_PATH = '../data/movies_dataset.zip'
RAW_LOCAL_PATH = '../data/RAW/'
CURATED_LOCAL_PATH = '../data/CURATED/'

TITLE_FILE_NAME = 'title.basics.tsv'

class IntermovieDataLoader:

    def split_data(self):
        '''
        Break raw data into many files
        '''
        
        with open(RAW_LOCAL_PATH + TITLE_FILE_NAME, encoding='utf-8') as file_stream:    
            file_stream_reader = csv.DictReader(file_stream, delimiter='\t')
            
            open_files_references = {}

            for row in file_stream_reader:
                title_type = row['titleType']

                # Open a new file and write the header
                if title_type not in open_files_references:
                    output_file = open(CURATED_LOCAL_PATH + '{}.csv'.format(title_type), 'w', encoding='utf-8', newline='')
                    dictionary_writer = csv.DictWriter(output_file, fieldnames=file_stream_reader.fieldnames)
                    dictionary_writer.writeheader()
                    open_files_references[title_type] = output_file, dictionary_writer
                # Always write the row
                open_files_references[title_type][1].writerow(row)
            # Close all the files
            for output_file, _ in open_files_references.values():
                output_file.close()

    def ensure_data_loaded(self):
        '''
        Ensure if data are already loaded. Download if missing
        '''

        if path.exists(ZIP_LOCAL_PATH) == False:
            self._download_data()

        if len(listdir(RAW_LOCAL_PATH)) == 0:
            self._extract_data()

        print('Les fichiers sont correctement extraits')


    def _download_data(self):
        '''
        Download the data from internet
        '''
        
        print('Donwloading data')
        with open(ZIP_LOCAL_PATH, "wb") as f:
            response = requests.get(ZIP_REMOTE_PATH, stream=True)
            total_length = response.headers.get('content-length')

            if total_length is None: # no content length header
                f.write(response.content)
            else:
                dl = 0
                total_length = int(total_length)
            
                for data in response.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    done = int(50 * dl / total_length)
                    sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
                    sys.stdout.flush()

        print('Data download successfully')

        self._extract_data()

    def _extract_data(self):
        '''
        Extract the zip file to the hard disk
        '''

        print('Begin extracting data')
        with zipfile.ZipFile(ZIP_LOCAL_PATH, 'r') as zip_ref:
            zip_ref.extractall(RAW_LOCAL_PATH)
        print('Data extract successfully')
