import requests
import base64
import geojson
import os

import image_processing
# import file_upload

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
parent_id = 57

service_name = 'copernicus'  # захардкодено
polarization_type = None  # захардкодено

# тестовые данные
username_service = 'antan183'
username_password = 'antanantan'

if not os.path.isdir(download_directory):
    os.mkdir(download_directory)
# else:
#     for item in os.listdir(download_directory):
#         item_path = os.path.join(download_directory, item)
#
#         if os.path.isfile(item_path):
#             os.remove(item_path)
#             print(f"Удален файл: {item_path}")
#         elif os.path.isdir(item_path):
#             transform.clear_directory(item_path)
#             os.rmdir(item_path)
#             print(f"Удалена директория: {item_path}")

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

    image_processing.extract(download_directory)
    image_processing.transform_tiff(download_directory, boundary, webgis_addr, webgis_username, webgis_password, parent_id, polarization_type)

else:
    print(scenes['message'])