# -*- coding: utf-8 -*-

#******************************************************************************
#
# NGSatSearch
# ---------------------------------------------------------
# Module for searching and downloading satellite data
#
# Copyright (C) 2019 NextGIS (info@nextgis.org)
#
#******************************************************************************

import os
from datetime import datetime
import xml.etree.ElementTree as etree
import requests
import sys
from requests.auth import HTTPBasicAuth
from . import NGSatExceptions

class CopernicusDownloader():

    opensearch_base_url = 'https://scihub.copernicus.eu/dhus/search'
    odata_base_url = 'https://scihub.copernicus.eu/dhus/odata/v1/'

    platforms = [{'name':'Sentinel-1',
                  'options': ['sensoroperationalmode','producttype','polarisation','filename'],
                  'sensoroperationalmode':['SW','IW','EW','WV'],
                  'producttype':['SLC','GRD','OCN'],
                  'polarisationmode':['HH','HV','VH','VV','HH+HV','VV+VH'],
                  'filename':['S1A_*','S1B_*']},

                 {'name':'Sentinel-2',
                  'options': ['cloudcoverpercentage','producttype','filename'],
                  'cloudcoverpercentage':'string like [0 TO 9.4]',
                  'producttype':['S2MSI1C','S2MSI2A','S2MSI2Ap'],
                  'filename':['S2A_*','S2B_*']},

                 {'name':'Sentinel-3',
                  'options': ['producttype','filename'],
                  'producttype':['OL_1_EFR___','OL_1_ERR___','OL_2_LFR___',
                                 'OL_2_LRR___','SR_1_SRA___','SR_1_SRA_A_',
                                 'SR_1_SRA_BS','SR_2_LAN___','SL_1_RBT___',
                                 'SL_2_LST___','SY_2_SYN___','SY_2_V10___',
                                 'SY_2_VG1___','SY_2_VGP___'],
                  'filename':['S3A_*','S3B_*']}]

    def __init__(self, username, password, download_directory):
        self.username = username
        self.password = password
        self.download_directory = download_directory

    def search_by_conditions(self, platform, wkt_region=None, start_date=None, end_date=None, options=None):
        if options == None:
            options = []
        # Compile conditions to query
        conditions = []
        rows = 100
        platform_dict = [c_platform for c_platform in self.platforms if c_platform['name']==platform][0]
        if not platform_dict:
            raise NGSatExceptions.UnsupportedPlatform

        conditions.append('platformname: %s' % platform_dict['name'])

        if wkt_region:
            conditions.append('footprint:"Intersects(%s)"' % wkt_region)

        if start_date and not end_date:
            conditions.append('beginposition:[%s TO NOW]' % self.__datetime_to_scihub_format(start_date))
        if start_date and end_date:
            conditions.append('beginposition:[%s TO %s]' % (self.__datetime_to_scihub_format(start_date), self.__datetime_to_scihub_format(end_date)))
        if end_date and not start_date:
            conditions.append('endposition:[%s TO %s]' % (self.__datetime_to_scihub_format(datetime(year=1970, month=1, day=1)),self.__datetime_to_scihub_format(end_date)))

        options_str = ''
        for option in options:
            if not option['name'] in platform_dict['options']:
                raise NGSatExceptions.InvalidOption
            else:
                options_str = options_str + '%s:%s AND ' % (option['name'],option['value'])

        if len(options_str) > 0:
            options_str = '(%s)' % options_str[0:-5]
            conditions.append(options_str)

        conditions = ' AND '.join(conditions)

        # Begin querying
        datasource = requests.get(self.opensearch_base_url,
                                  params={'q': conditions, 'rows': str(rows), 'orderby': 'beginposition asc'},
                                  auth=HTTPBasicAuth(self.username, self.password))
        
        if datasource.status_code == 401:
            raise NGSatExceptions.AuthorizationError
        if datasource.status_code == 404:
            raise NGSatExceptions.ConnectionError

        try:
            number_of_results = int(etree.ElementTree(etree.fromstring(datasource.content)).getroot().find(
                '{http://a9.com/-/spec/opensearch/1.1/}totalResults').text)
        except:
            raise NGSatExceptions.InvalidMetadata


        list_of_identifiers = []

        answer_data = etree.ElementTree(etree.fromstring(datasource.content)).getroot()
        entries = answer_data.findall('{http://www.w3.org/2005/Atom}entry')

        for entry in entries:
            entry_identifier = self.__get_identifier_from_entry(entry)
            list_of_identifiers.append(entry_identifier)

        if number_of_results > rows:
            for start_row in range(rows, number_of_results, rows):
                #'requesting results from %s to %s' % (str(start_row), str(start_row + int(rows)))
                datasource = requests.get(self.opensearch_base_url,
                                          params={'q': conditions, 'start': str(start_row), 'rows': str(rows),
                                                  'orderby': 'beginposition asc'},
                                          auth=HTTPBasicAuth(self.username, self.password))

                answer_data = etree.ElementTree(etree.fromstring(datasource.content)).getroot()
                entries = answer_data.findall('{http://www.w3.org/2005/Atom}entry')
                for entry in entries:
                    entry_identifier = self.__get_identifier_from_entry(entry)
                    list_of_identifiers.append(entry_identifier)

        return list_of_identifiers

    def download_by_identifier(self, identifier, stdout = False, custom_name = None, bands=None, metadata_needed=None, download_extra_files=False):
        if custom_name == None:
            filename = identifier + '.zip'
        else:
            filename = custom_name

        downloading_path = os.path.join(self.download_directory,filename)

        uuid = self.__get_uuid_by_identifier(identifier)

        downloader_string = self.odata_base_url + 'Products(\'%s\')/$value' % str(uuid)
        r = requests.get(downloader_string, auth=HTTPBasicAuth(self.username, self.password), stream=True)

        if r.status_code == 401:
            raise NGSatExceptions.AuthorizationError
        if r.status_code == 404:
            raise NGSatExceptions.DatasetNotFound

        current_size = 0
        chunk_size = 4096
        start_time = datetime.now()

        if stdout:
            sys.stdout.write('Downloading %s\n' % filename)

        try:
            with open(downloading_path, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if stdout:
                        sys.stdout.write('Megabytes downloaded: %s\r' % str(current_size / 1024 / 1024.0))
                        sys.stdout.flush()
                    fd.write(chunk)
                    current_size += chunk_size

            end_time = datetime.now()
            if stdout:
                sys.stdout.write('\n')
                sys.stdout.write('Downloaded in %s seconds\n' % (end_time - start_time).total_seconds())

            return {'status': 'ok', 'code': 0, 'message': 'OK', 'data': [filename]}
        except:
            return {'status': 'error', 'code': 1, 'message': 'Nothing was downloaded'}


    def get_metadata_by_identifier(self, identifier, as_text=True):
        datasource = requests.get(self.opensearch_base_url,
                                  params={'q': identifier, 'rows': 1, 'orderby': 'beginposition asc'},
                                  auth=HTTPBasicAuth(self.username, self.password))

        answer_data = etree.ElementTree(etree.fromstring(datasource.content)).getroot()
        entries = answer_data.findall('{http://www.w3.org/2005/Atom}entry')
        if not entries:
            raise NGSatExceptions.DatasetNotFound
        if as_text:
            return etree.tostring(entries[0]).decode()
        else:
            return entries[0]

    def __get_uuid_by_identifier(self, identifier):
        metadata = self.get_metadata_by_identifier(identifier, as_text=False)
        uuid = ''

        for child in metadata:
            if child.get('name') == 'uid':
                uid = child.text

        if not uuid:
            uuid = metadata.find('{http://www.w3.org/2005/Atom}id').text

        if not uuid:
            raise NGSatExceptions.InvalidMetadata('No uuid found in metadata')

        return uuid


    def __datetime_to_scihub_format(self, user_datetime):
        return user_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    def __get_identifier_from_entry(self, entry):
        identifier = ''
        for child in entry:
            if child.get('name') == 'identifier':
                identifier = child.text

        if not identifier:
            identifier = entry.find('{http://www.w3.org/2005/Atom}title').text

        if not identifier:
            raise NGSatExceptions.InvalidMetadata('No identifier found in entry')

        return identifier