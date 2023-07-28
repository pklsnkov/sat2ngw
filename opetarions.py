import requests
import base64
import multiprocessing
from NGSatSearch.NGSatSearch import NGSatSearch  # https://gitlab.com/nextgis_private/ngsatsearch
from datetime import datetime

import os
import transform

# исходные данные на вход
boundary = 'boundary.geojson'
catalog = ''
platform = ''
satt_imagery_type = ''
webgis_addr = ''
webgis_username = ''
webgis_password = ''
download_directory = 'images'

service_name = 'copernicus'  # захардкодено
polarization_type = None  # захардкодено

# тестовые данные
username_service = 'antan'
username_password = 'kYTC82.nd5&EsXx'

if not os.path.isdir(download_directory):
    os.mkdir(download_directory)

ngss = NGSatSearch(service_name=service_name,
                   username=username_service,
                   password=username_password,
                   download_directory=download_directory)

ngss.get_available_platforms()

options = [
    {'name': 'sensoroperationalmode', 'value': 'IW'},
    {'name': 'producttype', 'value': 'GRD'}
]

scenes = ngss.search_by_conditions(platform='Sentinel-1',
                                   ogr_source=boundary,
                                   start_date=datetime(2023, 7, 1),
                                   end_date=datetime(2023, 7, 20),
                                   options=options)

if scenes['code'] == 0:
    ids = scenes['data']
    # print(scenes)
    for id in ids:
        ngss.download_by_identifier(identifier=id)

    transform.extract(download_directory)
    transform.transform_tiff(download_directory, polarization_type)

else:
    raise print('Неверные данные', scenes['code'])


#///

# загрузка данных в NGW (https://docs.nextgis.ru/docs_ngweb_dev/doc/developer/file_upload.html#multiple-file-upload)

# upload_url = f'{webgis_addr}/api/component/file_upload/'
# upload_dir = 'images_reproj'
# creds = f"{webgis_username}:{webgis_password}"
# headers = {
#             'Accept': '*/*',
#             'Authorization': 'Basic' + ' ' + base64.b64encode(creds.encode("utf-8")).decode("utf-8")
#         }
# response = requests.post(url=upload_url,
#                          files=upload_files,
#                          headers=headers)


