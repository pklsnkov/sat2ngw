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
import requests
import sys
import json
from . import NGSatExceptions

class EarthExplorerDownloader():
    api_base_url = 'https://earthexplorer.usgs.gov/inventory/json/v/1.4.0/'

    platforms = [{'name': 'LANDSAT_8_C1',
                  'options': ['minCloudCover', 'maxCloudCover'],
                  'minCloudCover': 'integer',
                  'maxCloudCover': 'integer'},

                 {'name': 'LANDSAT_ETM_C1',
                  'options': ['minCloudCover', 'maxCloudCover'],
                  'minCloudCover': 'integer',
                  'maxCloudCover': 'integer'},

                 {'name': 'LANDSAT_TM_C1',
                  'options': ['minCloudCover', 'maxCloudCover'],
                  'minCloudCover': 'integer',
                  'maxCloudCover': 'integer'},

                 {'name': 'LANDSAT_MSS_C1',
                  'options': ['minCloudCover', 'maxCloudCover'],
                  'minCloudCover': 'integer',
                  'maxCloudCover': 'integer'}]

    def __init__(self, username, password, download_directory):
        self.username = username
        self.password = password
        self.download_directory = download_directory

        self.api_key = self.__get_api_key()

    def __get_api_key(self):
        request_body = {"username":self.username, "password": self.password}
        request_url = self.__build_request('Login',request_body)
        r = requests.get(request_url)
        response = json.loads(r.content)
        if response['errorCode'] == 'AUTH_INVALID':
            raise NGSatExceptions.AuthorizationError
        return response['data']

    def search_by_conditions(self, platform, wkt_region=None, start_date=None, end_date=None, options=None):
        if options == None:
            options = []

        request_body = {}
        rows = 100

        platform_dict = [c_platform for c_platform in self.platforms if c_platform['name'] == platform][0]
        if not platform_dict:
            raise NameError('Unsupported platform')

        if start_date or end_date:
            request_body['temporalFilter'] = {}

        if start_date:
            request_body['temporalFilter']['startDate'] = self.__datetime_to_ee_format(start_date)
        if end_date:
            request_body['temporalFilter']['endDate'] = self.__datetime_to_ee_format(end_date)

        if wkt_region:
            bbox = self.__get_extent_of_wkt_polygon(wkt_region)
            request_body['spatialFilter'] = {"filterType": "mbr",
                                             "lowerLeft": {
                                                 "latitude": bbox['yMin'],
                                                 "longitude": bbox['xMin'],
                                                },
                                             "upperRight": {
                                                 "latitude": bbox['yMax'],
                                                 "longitude": bbox['xMax'],
                                                }
                                             }

        request_body['apiKey'] = self.api_key
        request_body["datasetName"] = platform_dict['name']
        request_body["maxResults"] = rows
        request_body["startingNumber"] = 1

        for option in options:
            request_body[option['name']] = option['value']

        request_url = self.__build_request('search',request_body)

        r = requests.get(request_url)
        response = json.loads(r.content)

        if response['errorCode'] == None:
            list_of_identifiers = []
            number_of_results = response['data']['totalHits']
            for item in response['data']['results']:
                list_of_identifiers.append(item['displayId'])

            if number_of_results > rows:
                for start_row in range(rows+1, number_of_results, rows):
                    # 'requesting results from %s to %s' % (str(start_row), str(start_row + int(rows)))
                    request_body["startingNumber"] = start_row
                    request_url = self.__build_request('search', request_body)
                    r = requests.get(request_url)
                    response = json.loads(r.content)
                    for item in response['data']['results']:
                        list_of_identifiers.append(item['displayId'])
        elif response['errorCode'] == 'AUTH_UNAUTHORIZED':
            raise NGSatExceptions.AuthorizationError
        else:
            raise NGSatExceptions.QueryError

        return list_of_identifiers

    def download_by_identifier(self, identifier, stdout = False, custom_name = None, bands=None, metadata_needed=None, download_extra_files=False):
        datasetName = self.__detect_datasetName_by_identifier(identifier)
        entityId = self.__get_entityId_by_displayId(identifier, datasetName)

        if custom_name == None:
            filename = identifier + '.tar.gz'
        else:
            filename = custom_name

        request_body = {}
        request_body['apiKey'] = self.api_key
        request_body['datasetName'] = datasetName
        request_body['products'] = ["STANDARD"]
        request_body['entityIds'] = [entityId]

        request_url = self.__build_request('download', request_body)
        r = requests.get(request_url)
        response = json.loads(r.content)
        if response['errorCode'] == 'AUTH_UNAUTHORIZED':
            raise NGSatExceptions.AuthorizationError

        if len(response['data']) == 0:
            raise NGSatExceptions.InvalidIdentifier

        download_url = response['data'][0]['url']
        downloading_path = os.path.join(self.download_directory, filename)

        r = requests.get(download_url, stream=True)

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

            return {'status': 'ok', 'code': 0, 'message': 'OK', 'data':[filename]}
        except:
            return {'status': 'error', 'code': 1, 'message': 'Nothing was downloaded'}


    def get_metadata_by_identifier(self, identifier):
        datasetName = self.__detect_datasetName_by_identifier(identifier)
        displayId_fieldId = self.__get_fieldId_for_displayId(datasetName)

        request_body = {}
        request_body['apiKey'] = self.api_key
        request_body['datasetName'] = datasetName
        request_body['additionalCriteria'] = {"filterType": "value", "fieldId":displayId_fieldId,"value":identifier, "operand": "like"}

        request_url = self.__build_request('search', request_body)
        r = requests.get(request_url)
        response = json.loads(r.content)
        if response['errorCode'] == 'AUTH_UNAUTHORIZED':
            raise NGSatExceptions.AuthorizationError
        if response['errorCode'] == 'METADATA_SCENES_INVALID':
            raise NGSatExceptions.InvalidIdentifier

        metadata = response['data']['results'][0]
        return metadata


    def __get_entityId_by_displayId(self, displayId, datasetName):
        request_body = {}
        request_body['apiKey'] = self.api_key
        request_body['datasetName'] = datasetName
        request_body['idList'] = [displayId]
        request_body['inputField'] = 'displayId'

        request_url = self.__build_request('idlookup', request_body)
        r = requests.get(request_url)
        response = json.loads(r.content)
        if response['errorCode'] == 'AUTH_UNAUTHORIZED':
            raise NGSatExceptions.AuthorizationError

        entityId = response['data'][displayId]
        if not entityId:
            raise NGSatExceptions.InvalidIdentifier
        return entityId

    def __get_fieldId_for_displayId(self, datasetName):
        request_body = {}
        request_body['apiKey'] = self.api_key
        request_body['datasetName'] = datasetName
        request_url = self.__build_request('datasetfields', request_body)
        r = requests.get(request_url)
        response = json.loads(r.content)
        if response['errorCode'] == 'AUTH_UNAUTHORIZED':
            raise NGSatExceptions.AuthorizationError
        fieldId = None

        data = response['data']
        for item in data:
            if item['name'] == 'Landsat Product Identifier':
                fieldId = item['fieldId']

        return fieldId


    def __detect_datasetName_by_identifier(self, identifier):
        if identifier.startswith('LC08'):
            return 'LANDSAT_8_C1'
        elif identifier.startswith('LE07'):
            return 'LANDSAT_ETM_C1'
        elif (identifier.startswith('LT05') or identifier.startswith('LT04')):
            return 'LANDSAT_TM_C1'
        elif (identifier.startswith('LM01') or identifier.startswith('LM02') or identifier.startswith('LM03') or
              identifier.startswith('LM04') or identifier.startswith('LM05')):
            return 'LANDSAT_MSS_C1'
        else:
            raise NGSatExceptions.UnsupportedPlatform

    def __build_request(self, request, json_body):
        request_url = '%s%s?jsonRequest=%s' % (self.api_base_url, request, str(json_body).replace('\'','\"'))
        return request_url

    def __datetime_to_ee_format(self, user_datetime):
        return user_datetime.strftime("%Y-%m-%d")

    def __get_extent_of_wkt_polygon(self, polygon_wkt):
        try:
            start_coordinates = polygon_wkt.find('((') + 2
            end_coordinates = polygon_wkt.find('))')
            points_str = polygon_wkt[start_coordinates:end_coordinates]

            points = points_str.split(',')
            points = [point.lstrip() for point in points]

            x_min = None
            y_min = None
            x_max = None
            y_max = None

            for point in points:
                x = float(point.split(' ')[0])
                y = float(point.split(' ')[1])
                if (x_min == None) or (x < x_min):
                    x_min = x
                if (x_max == None) or (x > x_max):
                    x_max = x
                if (y_min == None) or (y < y_min):
                    y_min = y
                if (y_max == None) or (y > y_max):
                    y_max = y
        except:
            raise NGSatExceptions.InvalidPolygon

        return {'xMax': x_max, 'xMin': x_min, 'yMax': y_max, 'yMin': y_min}
