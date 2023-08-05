import requests
import os
import base64
import re
from tusclient.client import TusClient
import glob
from contextlib import closing


def file_upload(webgis_addr, webgis_username, webgis_password):

    images_directory = 'transformed_image'
    creds = f"{webgis_username}:{webgis_password}"

    upload_dirs = [folder for folder in os.listdir(images_directory)]

    children_list = []

    for upload_dir in upload_dirs:
        upload_dir_path = os.path.join(images_directory, upload_dir)
        # upload_files = [os.path.join(upload_dir_path, file) for file in os.listdir(upload_dir_path)]

        print('Директория для загрузки выбран')

        tiff_file = [file for file in os.listdir(upload_dir_path) if file.endswith('.tiff')][0]
        tiff_file_path = os.path.join(images_directory, upload_dir, tiff_file)

        style_file = [file for file in os.listdir(upload_dir_path) if file.endswith('.qml')][0]
        style_file_path = os.path.join(images_directory, upload_dir, style_file)

        tiff_meta = uploading_file(webgis_addr, creds, tiff_file_path)
        raster_layer_id = create_raster_layer(webgis_addr, creds, tiff_file, tiff_meta)

        style_meta = uploading_file(webgis_addr, creds, style_file_path)
        raster_style_id = create_raster_style(webgis_addr, creds, style_file, style_meta, raster_layer_id)

        child_dict = {
            "layer_enabled": False,
            "layer_adapter": "tile",
            "display_name": tiff_file,
            "layer_style_id": raster_style_id,
            "item_type": "layer"
        }

        children_list.append(child_dict)

    create_webmap(webgis_addr, creds, children_list)


def uploading_file(webgis_addr, creds, file):
    upload_url = f'{webgis_addr}/api/component/file_upload/'

    headers = {
        'Accept': '*/*',
        'Authorization': 'Basic' + ' ' + base64.b64encode(creds.encode("utf-8")).decode("utf-8")
    }

    filename = os.path.basename(file).replace('_cut.tiff', '')
    # form_data = {
    #     'file': (filename, open(file, 'rb')),
    #     'name': filename
    # }

    # raw_file_path = file.replace('\\\\', '\\')
    # print(raw_file_path)
    #
    # form_data = f"file={raw_file_path}&name={filename}"
    # response = requests.post(url=upload_url,
    #                          data=form_data,
    #                          headers=headers)

    with open(file, 'rb') as f:
        files = {'files[]': (file, f)}
        form_data = {
            'name': [file, f]
        }

        # todo : проблема в этом запросе, варианты: рав-строка, тусклиент, можно попробовать переписать и запустить в нгбат

        response = requests.post(url=upload_url,
                                 files=files,
                                 data=form_data,
                                 headers=headers)

    if response.status_code == 200:
        print('Загрузка завершена', filename)
        json_response = response.json()
        upload_meta = json_response.get('upload_meta', [])
    else:
        print(response.status_code)

    return upload_meta


def create_raster_layer(webgis_addr, creds, filename, upload_meta):

    create_url = f'{webgis_addr}/api/resource/'

    resource = {
        "resource": {
            "cls": "raster_layer",
            "display_name": filename,
            "parent": {"id": 0}
        },
        "raster_layer": {
            "source": upload_meta,
            "srs": {"id": 3857}
        }
    }

    headers = {
        'Accept': '*/*',
        'Authorization': 'Basic' + ' ' + base64.b64encode(creds.encode("utf-8")).decode("utf-8")
    }

    response = requests.post(url=create_url,
                             headers=headers,
                             json=resource)

    if response.status_code != 201:
        print('Crashed: impossible to create style at NGW. Status: %s' % response.text)
    else:
        print(response.text)

    return response.json()['id']


def create_raster_style(webgis_addr, creds, filename, upload_meta, raster_layer_id):

    create_url = f'{webgis_addr}/api/resource/'

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

    headers = {
        'Accept': '*/*',
        'Authorization': 'Basic' + ' ' + base64.b64encode(creds.encode("utf-8")).decode("utf-8")
    }

    response = requests.post(url=create_url,
                             headers=headers,
                             json=resource)
    if response.status_code != 201:
        print('Crashed: impossible to create style at NGW. Status: %s' % response.text)
    else:
        print(response.text)

    return response.json()['id']


def create_webmap(webgis_addr, creds, children_list):

    create_url = f'{webgis_addr}/api/resource/'

    webmap_json = {
        "resource": {
            "display_name": "TIFF_FILES",
            "parent": {
                "id": 0
            },
            "cls": "webmap"
        },
        "webmap": {
            "root_item": {
                "item_type": "root",
                "children": children_list
            }
        }
    }



    headers = {
        'Accept': '*/*',
        'Authorization': 'Basic' + ' ' + base64.b64encode(creds.encode("utf-8")).decode("utf-8")
    }

    responce_webmap = requests.post(url=create_url,
                                    headers=headers,
                                    json=webmap_json)

    if responce_webmap.status_code < 200 and responce_webmap.status_code > 300:
        raise responce_webmap.status_code



file_upload('https://kolesnikov-p.nextgis.com',
            'pvk200815@gmail.com',
            'yNCY3VQ4zNDDYJ4')
