# загрузка данных в NGW (https://docs.nextgis.ru/docs_ngweb_dev/doc/developer/file_upload.html#multiple-file-upload)

import requests
import os
import math
from tusclient.client import TusClient
from urllib.parse import urljoin


def file_upload(webgis_addr, webgis_username, webgis_password, images_directory, parent_id=0):

    creds = (webgis_username, webgis_password)

    upload_dirs = [folder for folder in os.listdir(images_directory)]

    for upload_dir in upload_dirs:
        upload_dir_path = os.path.join(images_directory, upload_dir)

        print(f'Загрузка началась, директория {upload_dir}')

        tiff_file = [file for file in os.listdir(upload_dir_path) if file.endswith('.tiff')][0]
        tiff_file_path = os.path.join(images_directory, upload_dir, tiff_file)

        style_file = [file for file in os.listdir(upload_dir_path) if file.endswith('.qml')][0]
        style_file_path = os.path.join(images_directory, upload_dir, style_file)

        tiff_meta = uploading_file(webgis_addr, creds, tiff_file_path)
        raster_layer_id = create_raster_layer(webgis_addr, creds, tiff_file, tiff_meta, parent_id)

        style_meta = uploading_file(webgis_addr, creds, style_file_path)
        raster_style_id = create_raster_style(webgis_addr, creds, style_file, style_meta, raster_layer_id)

        print(f'Загрузка завершена')

        render_url = raster_style_preview(webgis_addr, raster_style_id)

        upload_dir_basename = os.path.basename(upload_dir)
        parts = upload_dir_basename.split('-')
        for part in parts:
            split_part = part.split('T')
            if len(split_part) == 2:
                shooting_time_source = split_part[0]
                year = shooting_time_source[:4]
                month = shooting_time_source[4:6]
                day = shooting_time_source[6:]
                shooting_time = f'{day}.{month}.{year}'
                break

        message_text = (f"Загрузка снимка в NGW завершена,\n"
                        f"Идентификатор сцены: {upload_dir_basename}\n"
                        f"Дата съёмки: {shooting_time}\n"
                        f"Ссылка на превью в NextGIS Web: {webgis_addr}/resource/{raster_style_id}/preview")

        tg_message(method='sendPhoto',
                   token='6363573328:AAGLrbZtHy8hkZ6_E0pa_bsRb9fLXRkuIXI',
                   chat_id='-1001989735558',
                   text=message_text,
                   preview_path=f'tmp\\{raster_style_id}.png'
                   )


def uploading_file(webgis_addr, creds, file):
    tus_upload_path = '/api/component/file_upload/'
    chunk_size = 4 * 2 ** 20

    tus_client = TusClient(urljoin(webgis_addr, tus_upload_path))
    uploader = tus_client.uploader(file, metadata=dict(meta='data'), chunk_size=chunk_size)
    uploader.upload()
    furl = uploader.url

    response = requests.get(furl, auth=creds, json=True)
    upload_meta = response.json()

    return upload_meta


def create_raster_layer(webgis_addr, creds, filename, upload_meta, parent_id):
    resource = {
        "resource": {
            "cls": "raster_layer",
            "display_name": filename,
            "parent": {"id": parent_id}
        },
        "raster_layer": {
            "source": upload_meta,
            "srs": {"id": 3857}
        }
    }

    root = "%s/api/resource/" % (webgis_addr)
    response = requests.post(root, json=resource, auth=creds)
    if response.status_code != 201:
        print('Crashed: impossible to create raster at NGW. Status: %s' % response.text)
    else:
        print(response.text)

    return response.json()['id']


def create_raster_style(webgis_addr, creds, filename, upload_meta, raster_layer_id):
    resource = {
        "resource": {
            "cls": "qgis_raster_style",
            "display_name": filename,
            "parent": {"id": raster_layer_id}
        },
        "qgis_raster_style": {
            "file_upload": upload_meta
        }
    }

    root = "%s/api/resource/" % (webgis_addr)
    response = requests.post(root, json=resource, auth=creds)
    if response.status_code != 201:
        print('Crashed: impossible to create style at NGW. Status: %s' % response.text)
    else:
        print(response.text)

    return response.json()['id']

def raster_style_preview(webgis_addr, style_id):
    extent_url = f'{webgis_addr}/api/resource/{style_id}/extent'
    response = requests.get(extent_url)
    if response.status_code == 200:
        json_extent = response.json()
        extent_dict = {
            'minLon': json_extent['extent']['minLon'],
            'maxLon': json_extent['extent']['maxLon'],
            'minLat': json_extent['extent']['minLat'],
            'maxLat': json_extent['extent']['maxLat']
        }

        reproj_coords = {}
        for key, value in extent_dict.items():
            if 'Lon' in key:
                x = wgs84To3857X(value)
                reproj_coords[key] = x
            elif 'Lat' in key:
                y = wgs84To3857Y(value)
                reproj_coords[key] = y

    else:
        print(f'Error in coverage request, code {response.status_code}')

    render_url = (f"{webgis_addr}/api/component/render/image?"
                  f"resource={style_id}"
                  f"&extent={int(reproj_coords['minLon'])},{int(reproj_coords['minLat'])},{int(reproj_coords['maxLon'])},{int(reproj_coords['maxLat'])}"
                  f"&size=500,500")

    download_response = requests.get(render_url)

    if download_response.status_code == 200:
        with open(f'tmp\\previews\\{style_id}.png', 'wb') as file:
            file.write(download_response.content)


def wgs84To3857X(x):
    earthRadius = 6378137.0
    return earthRadius * math.radians(float(x))


def wgs84To3857Y(y):
    earthRadius = 6378137.0
    return earthRadius * math.log(
        math.tan(math.pi / 4 + math.radians(float(y)) / 2)
    )


def tg_message(method, token, chat_id, text, preview_path):
    session = requests.Session()
    response = session.post(
        url=f'https://api.telegram.org/bot{token}/{method}?chat_id={chat_id}',
        # data={'chat_id': chat_id, 'text': text}
        files={'photo': open(preview_path, 'rb')},
        data={'caption': text}
    ).json()

    print('Message delivered')