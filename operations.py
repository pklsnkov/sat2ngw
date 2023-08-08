import requests
import base64
import geojson
import os

import transform
import file_upload

import concurrent.futures

from shapely.geometry import shape
from datetime import datetime
from NGSatSearch.NGSatSearch import NGSatSearch  # https://gitlab.com/nextgis_private/ngsatsearch


# исходные данные на вход
boundary = 'boundary.geojson'
catalog = ''
platform = ''
satt_imagery_type = ''
webgis_addr = 'https://kolesnikov-p.nextgis.com'
webgis_username = 'pvk200815@gmail.com'
webgis_password = 'yNCY3VQ4zNDDYJ4'
download_directory = 'images'

service_name = 'copernicus'  # захардкодено
polarization_type = None  # захардкодено

# тестовые данные
username_service = 'antan183'
username_password = 'antanantan'

if not os.path.isdir(download_directory):
    os.mkdir(download_directory)

ngss = NGSatSearch(service_name=service_name,
                   username=username_service,
                   password=username_password,
                   download_directory=download_directory)

# ngss.get_available_platforms()

options = [
    {'name': 'sensoroperationalmode', 'value': 'IW'},
    {'name': 'producttype', 'value': 'GRD'}
]

with open(boundary, 'r') as f:
    data = geojson.load(f)

wkt_geometries = []

if data['type'] == 'FeatureCollection':
    for feature in data['features']:
        geometry = feature['geometry']
        wkt_geometries.append(shape(geometry).wkt)
else:
    geometry = data['geometry']
    wkt_geometries.append(shape(geometry).wkt)

wkt_data = '\n'.join(wkt_geometries)

scenes = ngss.search_by_conditions(platform='Sentinel-1',
                                   wkt_region=wkt_data,
                                   # ogr_source=boundary,
                                   start_date=datetime(2023, 7, 1),
                                   end_date=datetime(2023, 7, 20),
                                   options=options)

if scenes['code'] == 0:
    ids = scenes['data']

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_id = {executor.submit(fn=ngss.download_by_identifier, identifier=id): id for id in ids}
        for future in concurrent.futures.as_completed(future_to_id):
            id = future_to_id[future]
            try:
                future.result()
            except Exception as e:
                print(f"Ошибка для id: {id}: {e}")

    transform.extract(download_directory)
    transform.transform_tiff(download_directory, boundary, polarization_type)

else:
    print(scenes['message'])

# загрузка данных в NGW (https://docs.nextgis.ru/docs_ngweb_dev/doc/developer/file_upload.html#multiple-file-upload)

file_upload.file_upload(webgis_addr, webgis_username, webgis_password)
