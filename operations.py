import geojson
import re
import os
from urllib.parse import urlparse

import file_upload
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


# if webgis_addr.endswith('/'):
#     webgis_addr_len = len(webgis_addr)
#     webgis_addr = webgis_addr[:webgis_addr_len-1]
# if 'https://' not in webgis_addr:
#     webgis_addr = f'https://{webgis_addr}'
# result = urlparse(webgis_addr)
# if not all([result.scheme, result.netloc]):
#     raise ConnectionError('URL не валиден')

def make_valid_url(url):
    # beautify url taken from
    # https://github.com/nextgis/ngw_external_api_python/blob/master/qgis/ngw_connection_edit_dialog.py#L167

    url = url.strip()

    # Always remove trailing slashes (this is only a base url which will not be
    # used standalone anywhere).
    while url.endswith('/'):
        url = url[:-1]

    # Replace common ending when user copy-pastes from browser URL.
    url = re.sub('/resource/[0-9]+', '', url)

    o = urlparse(url)
    hostname = o.hostname

    # Select https if protocol has not been defined by user.
    if hostname is None:
        hostname = 'http://' if force_http else 'https://'
        return hostname + url

    # Force https regardless of what user has selected, but only for cloud connections.
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
else:
    image_processing.clear_directory(download_directory)

if not os.path.isdir('tmp'):
    os.mkdir('tmp')
else:
    image_processing.clear_directory('tmp')

if not os.path.isdir('tmp\\transformed_image'):
    os.mkdir('tmp\\transformed_image')
else:
    image_processing.clear_directory('tmp\\transformed_image')

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

scenes = ngss.search_by_conditions(platform='Sentinel-1',
                                   # wkt_region=wkt_data,
                                   ogr_source=boundary,
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
    image_processing.transform_tiff(folder=download_directory,
                                    boundary=boundary,
                                    webgis_addr=webgis_addr,
                                    webgis_username=webgis_username,
                                    webgis_password=webgis_password,
                                    parent_id=parent_id,
                                    polarization_type=polarization_type)

else:
    print(scenes['message'])
#
# file_upload.file_upload(webgis_addr=webgis_addr,
#                         webgis_username=webgis_username,
#                         webgis_password=webgis_password,
#                         images_directory='tmp\\transformed_image',
#                         parent_id=parent_id)

# image_processing.clear_directory('tmp')
# image_processing.clear_directory('transformed_image')
