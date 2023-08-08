# -*- coding: utf-8 -*-

#******************************************************************************
#
# NGSatSearch
# ---------------------------------------------------------
# Module for searching and downloading satellite data
#
# Copyright (C) 2019 NextGIS (info@nextgis.com)
#
#******************************************************************************

import os
from datetime import datetime
from datetime import timedelta
import xml.etree.ElementTree as etree
from osgeo import ogr
import requests
import sys
from requests.auth import HTTPBasicAuth
import json

from .CopernicusDownloader import CopernicusDownloader
from .EarthExplorerDownloader import EarthExplorerDownloader
from .GoogleCloudDownloader import GoogleCloudDownloader

from . import NGSatExceptions

class NGSatSearch():

    supported_services = ['copernicus', 'earthexplorer', 'google_cloud']

    def __init__(self, service_name, username, password, download_directory):
        self.change_download_directory(download_directory)
        self.change_service(service_name, username, password)


    def get_available_platforms(self):
        return self.downloader.platforms

    def change_service(self, service_name, username, password):
        if not service_name in self.supported_services:
            raise NameError('Unsupported service')

        self.service_name = service_name
        self.username = username
        self.password = password

        if service_name == 'copernicus':
            self.downloader = CopernicusDownloader(self.username, self.password, self.download_directory)
        elif service_name == 'earthexplorer':
            self.downloader = EarthExplorerDownloader(self.username, self.password, self.download_directory)
        elif service_name == 'google_cloud':
            self.downloader = GoogleCloudDownloader(self.username, self.password, self.download_directory)

    def change_download_directory(self, download_directory):
        if not os.path.exists(download_directory):
            try:
                os.makedirs(download_directory)
            except:
                raise PermissionError('Impossible to locate and create downloading directory')

        self.download_directory = download_directory


    def search_by_conditions(self, platform, wkt_region=None, ogr_source=None, start_date=None, end_date=None, options=None):
        """
        Method for searching identifiers with services APIs.

        :param platform: string from list. List could be obtained with get_available_platforms method
        :param wkt_region: Polygon geometry in WKT Format
        :param ogr_source: Path to polygonal OGR file
        :param start_date: date or datetime variable
        :param end_date: date or datetime variable
        :param options: list of options according to get_available_platforms return
        :return: dictionary with 'status', 'code', 'message' and 'data' keys. List of identifiers in 'data'
        """

        if options == None:
            options = []

        ### Processing of ogr_source: reproject to wgs84 and get only first feature
        if ogr_source != None:
            ogr_dataset = ogr.Open(ogr_source)
            if not ogr_dataset:
                return {'status': 'error', 'code': 1, 'message': 'Invalid OGR Datasource'}

            layer = ogr_dataset.GetLayer()
            if layer.GetGeomType() != 3:
                return {'status': 'error', 'code': 1, 'message': 'OGR Datasource must be polygon (not multipolygon, collection etc.)'}

            reprojected_ds = self.__reproject_ogr_dataset_to_projection(ogr_dataset, ogr.osr.SRS_WKT_WGS84_LAT_LONG, first_feature=True)
            reprojected_layer = reprojected_ds.GetLayer()
            if reprojected_layer.GetFeatureCount() == 0:
                return {'status': 'error', 'code': 1, 'message': 'No features were detected in OGR Datasource'}

            fet = reprojected_layer[0]
            geom = fet.geometry()
            wkt_region_d = geom.ExportToWkt()
        elif wkt_region != None:
            wkt_region_d = wkt_region
        else:
            wkt_region_d = None

        ##########################

        try:
            list_of_identifiers = self.downloader.search_by_conditions(platform, wkt_region=wkt_region_d, start_date=start_date, end_date=end_date, options=options)
        except NGSatExceptions.UnsupportedPlatform:
            return {'status':'error', 'code': 1,'message':'Unsupported platform. Supported: %s' % self.downloader.platforms}
        except NGSatExceptions.AuthorizationError:
            return {'status': 'error', 'code': 1, 'message': 'Authorization error. Check credentials'}
        except NGSatExceptions.InvalidMetadata:
            return {'status': 'error', 'code': 1, 'message': 'Invalid results obtained. Break. Possible reason: too much points in the given area of interest. Make sure it contains less than 10 points'}
        except NGSatExceptions.InvalidOption:
            return {'status': 'error', 'code': 1, 'message': 'Invalid options in request. See supported platforms: %s' % self.downloader.platforms}
        except NGSatExceptions.QueryError:
            return {'status': 'error', 'code': 1, 'message': 'Invalid query. Maybe, wrong options in request. See supported platforms: %s' % self.downloader.platforms}
        except NGSatExceptions.InvalidPolygon:
            return {'status': 'error', 'code': 1, 'message': 'Invalid WKT Polygon. Must be like POLYGON((30 60, 30 61, 31 61, 31 60, 30 60))'}
        except Exception as e:
            return {'status': 'error', 'code': 1, 'message': 'Error: %s' % str(e)}

        return {'status':'ok', 'code': 0, 'message':'OK','data':list_of_identifiers}

    def get_metadata_by_identifier(self, identifier):
        """
        Method for getting basic metadata of identifier from services APIs

        :param identifier: string with identifier
        :return: metadata as text
        """

        try:
            metadata = self.downloader.get_metadata_by_identifier(identifier)
        except NGSatExceptions.UnsupportedPlatform:
            return {'status': 'error', 'code': 1, 'message': 'Unsupported platform. Supported: %s' % self.downloader.platforms}
        except NGSatExceptions.DatasetNotFound:
            return {'status': 'error', 'code': 1, 'message': 'Dataset with given Identifier not found'}

        return {'status':'ok', 'code': 0, 'message':'OK','data':metadata}

    def download_by_identifier(self, identifier, stdout=True, custom_name=None, bands=None, metadata_needed=True, download_extra_files=False):
        """
        Method for downloading data by identifier from cloud services.

        :param identifier: string with identifier
        :param stdout: True for printing progress to sys.stdout
        :param custom_name: string, use for replacing default dataset name with custom name
        :param bands: list of bands. Use None for getting all bands. e.g. [4,5,'QA']
        :return: dictionary with 'status', 'code' and 'message' keys.
        """

        try:
            download = self.downloader.download_by_identifier(identifier, stdout=stdout, custom_name=custom_name,
                                                              bands=bands, metadata_needed=metadata_needed,
                                                              download_extra_files=download_extra_files)

        except NGSatExceptions.AuthorizationError:
            return {'status': 'error', 'code': 1, 'message': 'Authorization error'}
        except NGSatExceptions.DatasetNotFound:
            return {'status': 'error', 'code': 1, 'message': 'Dataset not found'}
        except NGSatExceptions.ServiceIsNotResponsible:
            return {'status': 'error', 'code': 1, 'message': 'Service is not responsible. It may be caused e.g. with Google Cloud temporal ban'}
        except Exception as e:
            return {'status': 'error', 'code': 1, 'message': 'Error: %s' % str(e)}
        return download

    def __reproject_ogr_dataset_to_projection(self, ogr_dataset, target_projection, first_feature=False):
        layer = ogr_dataset.GetLayer()
        sourceprj = layer.GetSpatialRef()
        targetprj = ogr.osr.SpatialReference(wkt=target_projection)
        try:
            targetprj.SetAxisMappingStrategy(ogr.osr.OAMS_TRADITIONAL_GIS_ORDER)
        except:
            pass
        transform = ogr.osr.CoordinateTransformation(sourceprj, targetprj)

        to_fill = ogr.GetDriverByName('memory')
        ds = to_fill.CreateDataSource('')
        outlayer = ds.CreateLayer('', targetprj, ogr.wkbPolygon)
        outlayer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        i = 0
        for feature in layer:
            transformed = feature.GetGeometryRef()
            transformed.Transform(transform)
            geom = ogr.CreateGeometryFromWkb(transformed.ExportToWkb())
            defn = outlayer.GetLayerDefn()
            feat = ogr.Feature(defn)
            feat.SetField('id', i)
            feat.SetGeometry(geom)
            outlayer.CreateFeature(feat)
            i += 1
            feat = None
            if first_feature == True:
                if i == 1:
                    break
        return ds