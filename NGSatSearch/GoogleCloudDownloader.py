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
import xml.etree.ElementTree as ET
from . import NGSatExceptions

class GoogleCloudDownloader():
    gc_base_url = 'http://storage.googleapis.com'

    platforms = [{'name': 'LANDSAT_8_C1', 'options': []},
                 {'name': 'LANDSAT_ETM_C1', 'options': []},
                 {'name': 'LANDSAT_TM_C1', 'options': []},
                 {'name': 'SENTINEL_2_L1C', 'options': []},
                 {'name': 'SENTINEL_2_L2A', 'options': []}]

    def __init__(self, username, password, download_directory):
        self.download_directory = download_directory

    def search_by_conditions(self, platform=None, wkt_region=None, start_date=None, end_date=None, options=None):
        # Search is not supported by GoogleCloud
        return []

    def download_by_identifier(self, identifier, stdout=True, custom_name=None, bands=None, metadata_needed=False, download_extra_files=False):
        urls = self.__get_google_cloud_urls_by_identifier(identifier, bands,
                                                          metadata_needed=metadata_needed, download_extra_files=download_extra_files)

        try:
            if custom_name:
                os.mkdir(os.path.join(self.download_directory, custom_name))
            else:
                os.mkdir(os.path.join(self.download_directory,identifier))
        except:
            pass

        downloaded_count = 0
        downloaded_list = []
        for url in urls:
            sys.stdout.write('Current url: %s\n' % url)
            filename = os.path.basename(url)
            downloading_path = os.path.join(self.download_directory,identifier,filename)
            if os.path.exists(downloading_path):
                if stdout:
                    sys.stdout.write('File %s already exists. Skip\n' % filename)
                continue

            r = requests.get(url, stream=True)
            if r.status_code != 200:
                if stdout:
                    sys.stdout.write('File %s was not found (status: %s)\n' % (filename, r.status_code))
                r.close()
                continue

            if stdout:
                sys.stdout.write('Downloading %s\n' % filename)

            current_size = 0
            chunk_size = 4096
            start_time = datetime.now()
            with open(downloading_path, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if stdout:
                        sys.stdout.write('Megabytes downloaded: %s\r' % str(current_size / 1024 / 1024.0))
                        sys.stdout.flush()
                    fd.write(chunk)
                    current_size += chunk_size
            downloaded_count += 1
            downloaded_list.append(filename)
            end_time = datetime.now()
            if stdout:
                sys.stdout.write('\n')
                sys.stdout.write('Downloaded in %s seconds\n' % (end_time - start_time).total_seconds())

        if downloaded_count > 0 and downloaded_count < len(urls):
            return {'status':'error', 'code': 2, 'message':'Some images were downloaded, but some not. See downloaded with \'data\' key', 'data': [downloaded_list]}
        if downloaded_count == 0:
            return {'status': 'error', 'code': 1, 'message': 'Nothing was downloaded'}
        if downloaded_count == len(urls):
            return {'status': 'ok', 'code': 0, 'message': 'OK', 'data': downloaded_list}


    def get_metadata_by_identifier(self, identifier):
        platform = self.__detect_platform_by_identifier(identifier)
        if platform == 'LANDSAT_8_C1':
            path = identifier.split('_')[2][0:3]
            row = identifier.split('_')[2][3:6]
            metadata_url = '%s/gcp-public-data-landsat/LC08/01/%s/%s/%s/%s_MTL.txt' % (self.gc_base_url, path, row, identifier, identifier)
        elif platform == 'LANDSAT_ETM_C1':
            path = identifier.split('_')[2][0:3]
            row = identifier.split('_')[2][3:6]
            metadata_url = '%s/gcp-public-data-landsat/LE07/01/%s/%s/%s/%s_MTL.txt' % (self.gc_base_url, path, row, identifier, identifier)
        elif platform == 'LANDSAT_TM_C1':
            path = identifier.split('_')[2][0:3]
            row = identifier.split('_')[2][3:6]
            sub_platform = identifier[0:4]
            metadata_url = '%s/gcp-public-data-landsat/%s/01/%s/%s/%s/%s_MTL.txt' % (self.gc_base_url, sub_platform, path, row, identifier, identifier)
        elif platform == 'SENTINEL_2_L1C':
            tile_block = identifier.split('_')[5]
            tile_l1 = tile_block[1:3]
            tile_l2 = tile_block[3]
            tile_l3 = tile_block[4:6]
            metadata_url = '%s/gcp-public-data-sentinel-2/tiles/%s/%s/%s/%s.SAFE/MTD_MSIL1C.xml' % (self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier)
        elif platform == 'SENTINEL_2_L2A':
            tile_block = identifier.split('_')[5]
            tile_l1 = tile_block[1:3]
            tile_l2 = tile_block[3]
            tile_l3 = tile_block[4:6]
            metadata_url = '%s/gcp-public-data-sentinel-2/L2/tiles/%s/%s/%s/%s.SAFE/MTD_MSIL2A.xml' % (self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier)
        else:
            raise NGSatExceptions.UnsupportedPlatform

        r = requests.get(metadata_url)
        if r.status_code == 404:
            raise NGSatExceptions.DatasetNotFound

        metadata = str(r.content)
        return metadata


    def __detect_platform_by_identifier(self, identifier):
        if identifier.startswith('LC08'):
            return 'LANDSAT_8_C1'
        elif identifier.startswith('LE07'):
            return 'LANDSAT_ETM_C1'
        elif (identifier.startswith('LT05') or identifier.startswith('LT04')):
            return 'LANDSAT_TM_C1'
        elif (identifier.find('MSIL1C_')) != -1:
            return 'SENTINEL_2_L1C'
        elif (identifier.find('MSIL2A_')) != -1:
            return 'SENTINEL_2_L2A'
        else:
            raise NGSatExceptions.UnsupportedPlatform

    def __get_available_bands_by_identifier(self, identifier):
        platform = self.__detect_platform_by_identifier(identifier)
        if platform == 'LANDSAT_8_C1':
            return [1,2,3,4,5,6,7,8,9,10,11,'QA']
        elif platform == 'LANDSAT_ETM_C1':
            return [1,2,3,4,5,'6_VCID_1','6_VCID_2',7,8,'QA']
        elif platform == 'LANDSAT_TM_C1':
            return [1,2,3,4,5,6,7,'QA']
        elif platform == 'SENTINEL_2_L1C':
            return [1, 2, 3, 4, 5, 6, 7, 8, '8A', 9, 10, 11, 12]
        elif platform == 'SENTINEL_2_L2A':
            return [1, 2, 3, 4, 5, 6, 7, 8, '8A', 9, 11, 12]


    def __get_google_cloud_urls_by_identifier(self, identifier, bands = None, metadata_needed=None, download_extra_files=False):
        platform = self.__detect_platform_by_identifier(identifier)

        if bands == None:
            bands = self.__get_available_bands_by_identifier(identifier)

        urls = []

        if platform in ['LANDSAT_8_C1','LANDSAT_ETM_C1','LANDSAT_TM_C1']:
            try:
                path = identifier.split('_')[2][0:3]
                row = identifier.split('_')[2][3:6]
                int(path)
                int(row)
            except:
                raise NGSatExceptions.InvalidIdentifier

        if platform == 'LANDSAT_8_C1':

            for band in bands:
                url = '%s/gcp-public-data-landsat/LC08/01/%s/%s/%s/%s_B%s.TIF' % (self.gc_base_url, path, row, identifier, identifier, band)
                urls.append(url)

            metadata_url = '%s/gcp-public-data-landsat/LC08/01/%s/%s/%s/%s_MTL.txt' % (self.gc_base_url, path, row, identifier, identifier)
            angles_url = '%s/gcp-public-data-landsat/LC08/01/%s/%s/%s/%s_ANG.txt' % (self.gc_base_url, path, row, identifier, identifier)
            if metadata_needed:
                urls.append(metadata_url)
                urls.append(angles_url)

        if platform == 'LANDSAT_ETM_C1':

            for band in bands:
                url = '%s/gcp-public-data-landsat/LE07/01/%s/%s/%s/%s_B%s.TIF' % (self.gc_base_url, path, row, identifier, identifier, band)
                urls.append(url)

            metadata_url = '%s/gcp-public-data-landsat/LE07/01/%s/%s/%s/%s_MTL.txt' % (self.gc_base_url, path, row, identifier, identifier)
            angles_url = '%s/gcp-public-data-landsat/LE07/01/%s/%s/%s/%s_ANG.txt' % (self.gc_base_url, path, row, identifier, identifier)
            if metadata_needed:
                urls.append(metadata_url)
                urls.append(angles_url)

        if platform == 'LANDSAT_TM_C1':
            sub_platform = identifier[0:4]

            for band in bands:
                url = '%s/gcp-public-data-landsat/%s/01/%s/%s/%s/%s_B%s.TIF' % (self.gc_base_url, sub_platform, path, row, identifier, identifier, band)
                urls.append(url)

            metadata_url = '%s/gcp-public-data-landsat/%s/01/%s/%s/%s/%s_MTL.txt' % (self.gc_base_url, sub_platform, path, row, identifier, identifier)
            angles_url = '%s/gcp-public-data-landsat/%s/01/%s/%s/%s/%s_ANG.txt' % (self.gc_base_url, sub_platform, path, row, identifier, identifier)
            if metadata_needed:
                urls.append(metadata_url)
                urls.append(angles_url)

        if platform in ['SENTINEL_2_L1C', 'SENTINEL_2_L2A']:
            try:
                tile_block = identifier.split('_')[5]
                tile_l1 = tile_block[1:3]
                tile_l2 = tile_block[3]
                tile_l3 = tile_block[4:6]
            except:
                raise NGSatExceptions.InvalidIdentifier

            # Get band urls from metadata
            if platform == 'SENTINEL_2_L1C':
                metadata_url = '%s/gcp-public-data-sentinel-2/tiles/%s/%s/%s/%s.SAFE/MTD_MSIL1C.xml' % (
                self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier)
            if platform == 'SENTINEL_2_L2A':
                metadata_url = '%s/gcp-public-data-sentinel-2/L2/tiles/%s/%s/%s/%s.SAFE/MTD_MSIL2A.xml' % (
                self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier)

            r = requests.get(metadata_url)
            if r.status_code == 404:
                raise NGSatExceptions.DatasetNotFound
            elif r.status_code != 200:
                raise NGSatExceptions.ServiceIsNotResponsible

            metadata = r.content
            try:
                parser_root = ET.fromstring(metadata)
            except:
                raise NGSatExceptions.InvalidMetadata
            images = []
            for image in parser_root.iter('IMAGE_FILE'):
                images.append(image.text)

            for band in bands:
                current_image = None
                for image in images:

                    if platform == 'SENTINEL_2_L1C':
                        if image.endswith(str(band).zfill(2)):
                            current_image = image

                    if platform == 'SENTINEL_2_L2A':
                        if band in [2, 3, 4, 8, '2', '3', '4', '8']:
                            if image.endswith(str(band).zfill(2) + '_10m'):
                                current_image = image
                        if band in [5, 6, 7, '8A', 11, 12, '5', '6', '7', '11', '12']:
                            if image.endswith(str(band).zfill(2) + '_20m'):
                                current_image = image
                        if band in [9, '9']:
                            if image.endswith(str(band).zfill(2) + '_60m'):
                                current_image = image

                if not current_image:
                    continue
                if platform == 'SENTINEL_2_L1C':
                    url = '%s/gcp-public-data-sentinel-2/tiles/%s/%s/%s/%s.SAFE/%s.jp2' % (
                    self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier, current_image)
                if platform == 'SENTINEL_2_L2A':
                    url = '%s/gcp-public-data-sentinel-2/L2/tiles/%s/%s/%s/%s.SAFE/%s.jp2' % (
                    self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier, current_image)

                urls.append(url)
            if metadata_needed:
                urls.append(metadata_url)

            if platform == 'SENTINEL_2_L1C':
                manifest_url = '%s/gcp-public-data-sentinel-2/tiles/%s/%s/%s/%s.SAFE/manifest.safe' % (
                self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier)
            if platform == 'SENTINEL_2_L2A':
                manifest_url = '%s/gcp-public-data-sentinel-2/L2/tiles/%s/%s/%s/%s.SAFE/manifest.safe' % (
                self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier)
            if metadata_needed:
                urls.append(manifest_url)

            if platform == 'SENTINEL_2_L2A':
                if download_extra_files == True:
                    if len(images) > 0:
                        cloud_mask_file = '%s/gcp-public-data-sentinel-2/L2/tiles/%s/%s/%s/%s.SAFE/%s' % (self.gc_base_url, tile_l1, tile_l2, tile_l3, identifier, os.path.relpath(os.path.join(os.path.dirname(images[0]),os.pardir,os.pardir,'QI_DATA','MSK_CLDPRB_60m.jp2')))
                        urls.append(cloud_mask_file.replace('\\','/'))

        return urls