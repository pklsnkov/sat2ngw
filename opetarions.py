import requests
import base64
import concurrent.futures
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

scenes = ngss.search_by_conditions(platform='Sentinel-1',
                                   ogr_source=boundary,
                                   start_date=datetime(2023, 7, 1),
                                   end_date=datetime(2023, 7, 20),
                                   options=options)

if scenes['code'] == 0:
    ids = scenes['data']
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_id = {executor.submit(ngss.download_by_identifier, identifier=id): id for id in ids}

        for future in concurrent.futures.as_completed(future_to_id):
            id = future_to_id[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred for id {id}: {e}")

    transform.extract(download_directory)
    transform.transform_tiff(download_directory, polarization_type)

else:
    print(scenes['message'])


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


