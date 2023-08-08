import requests
import os
import base64
import re
from tusclient.client import TusClient
from urllib.parse import urljoin
import glob
from contextlib import closing


def file_upload(webgis_addr, webgis_username, webgis_password):

    images_directory = 'transformed_image'
    # creds = f"{webgis_username}:{webgis_password}"
    creds = (webgis_username, webgis_password)

    upload_dirs = [folder for folder in os.listdir(images_directory)]

    # children_list = []

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

def create_raster_layer(webgis_addr, creds, filename, upload_meta):
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



file_upload('https://kolesnikov-p.nextgis.com',
            'pvk200815@gmail.com',
            'yNCY3VQ4zNDDYJ4')
