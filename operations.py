import geojson
import re
import os
from urllib.parse import urlparse

import file_upload
import image_processing

import concurrent.futures

from shapely.geometry import shape
from datetime import datetime
from datetime import date

from NGSatSearch.NGSatSearch import NGSatSearch  # https://gitlab.com/nextgis_private/ngsatsearch

# исходные данные на вход
boundary = 'boundary.geojson'
catalog = ''
platform = ''
satt_imagery_type = ''
webgis_addr = ''
webgis_username = ''
webgis_password = ''
parent_id = 0

service_name = 'copernicus'  # захардкодено
polarization_type = None  # захардкодено
download_directory = 'images'  # захардкодено

# тестовые данные
username_service = 'antan183'
username_password = 'antanantan'

def make_valid_url(url):

    url = url.strip()

    while url.endswith('/'):
        url = url[:-1]

    url = re.sub('/resource/[0-9]+', '', url)

    o = urlparse(url)
    hostname = o.hostname

    if hostname is None:
        hostname = 'http://' if force_http else 'https://'
        return hostname + url

    if url.startswith('http://') and url.endswith('.nextgis.com') and not force_http:
        return url.replace('http://', 'https://')

    return url


def json_to_wkt(json):
    with open(json, 'r') as f:
        data = geojson.load(f)

    wkt_geometries = []

    if data['type'] == 'FeatureCollection':
        for feature in data['features']:
            geometry = feature['geometry']
            wkt_geometries.append(shape(geometry).wkt)
    else:
        geometry = data['geometry']
        wkt_geometries.append(shape(geometry).wkt)

    return '\n'.join(wkt_geometries)


if not os.path.isdir(download_directory):
    os.mkdir(download_directory)
# else:
#     image_processing.clear_directory(download_directory)

if not os.path.isdir('tmp'):
    os.mkdir('tmp')
# else:
#     image_processing.clear_directory('tmp')

if not os.path.isdir('tmp\\transformed_image'):
    os.mkdir('tmp\\transformed_image')
if not os.path.isdir('tmp\\previews'):
    os.mkdir('tmp\\previews')
# else:
#     image_processing.clear_directory('tmp\\transformed_image')

force_http = False
if webgis_addr.startswith('http://'): force_http = True
make_valid_url(webgis_addr)

wkt_data = json_to_wkt(boundary)

ngss = NGSatSearch(service_name=service_name,
                   username=username_service,
                   password=username_password,
                   download_directory=download_directory)

options = [
    {'name': 'sensoroperationalmode', 'value': 'IW'},
    {'name': 'producttype', 'value': 'GRD'}
]

current_datetime = date.today()
now_year = current_datetime.year
now_month = current_datetime.month
now_day = current_datetime.day

scenes = ngss.search_by_conditions(platform='Sentinel-1',
                                   # wkt_region=wkt_data,
                                   ogr_source=boundary,
                                   start_date=datetime(now_year, now_month, now_day-7),
                                   end_date=datetime(now_year, now_month, now_day+1),
                                   options=options)

if scenes['code'] == 0:
    search_ids = scenes['data']

    with open("downloaded_data.txt", "r") as file:
        downloaded_data = set(line.strip() for line in file)

    ids = [data for data in search_ids if data not in downloaded_data]
    if len(ids) == 0:
        print('No new images were found')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_id = {executor.submit(fn=ngss.download_by_identifier, identifier=id): id for id in ids}
        for future in concurrent.futures.as_completed(future_to_id):
            id = future_to_id[future]
            try:
                future.result()
                downloaded_data.add(id)
            except Exception as e:
                print(f"Error for id: {id}: {e}")

    with open("downloaded_data.txt", "w") as file:
        for data in downloaded_data:
            file.write(data + "\n")

    image_processing.extract(download_directory)
    image_processing.transform_tiff(folder=download_directory,
                                    boundary=boundary,
                                    webgis_addr=webgis_addr,
                                    webgis_username=webgis_username,
                                    webgis_password=webgis_password,
                                    parent_id=parent_id,
                                    polarization_type=polarization_type)

else:
    print(scenes['message'])

file_upload.file_upload(webgis_addr=webgis_addr,
                        webgis_username=webgis_username,
                        webgis_password=webgis_password,
                        images_directory='tmp\\transformed_image',
                        parent_id=parent_id)

# image_processing.clear_directory('tmp')
image_processing.clear_directory('tmp\\transformed_image')
image_processing.clear_directory('tmp\\previews')
image_processing.clear_directory('images')
