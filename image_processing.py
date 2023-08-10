import os
import shutil
import zipfile
from osgeo import gdal, ogr, osr
import pyproj
import json

import numpy as np

import file_upload

import xml.etree.ElementTree as ET


def extract(folder):
    dir_list = os.listdir(folder)
    for directory in dir_list:
        dir_path = os.path.join(folder, directory)
        if '.zip' in dir_path:
            try:
                with zipfile.ZipFile(dir_path, 'r') as zip_open:
                    zip_open.extractall(folder)
                    os.remove(dir_path)
                    print('Unpacking was successful')
            except:
                os.remove(dir_path)


def transform_tiff(folder, boundary, webgis_addr, webgis_username, webgis_password, parent_id, polarization_type=None):

    for attribute_folder in os.listdir(folder):
        measurement_folder = os.path.join(folder, attribute_folder, 'measurement')
        if polarization_type is None:
            tiff_file = os.path.join(measurement_folder, os.listdir(measurement_folder)[0])
            tiff_file_name, tiff_file_ext = os.path.splitext(os.path.basename(tiff_file))
        else:
            for file in os.listdir(measurement_folder):
                if polarization_type in file:
                    tiff_file = os.path.join(measurement_folder, file)
                    tiff_file_name, tiff_file_ext = os.path.splitext(os.path.basename(tiff_file))
                else:
                    print('Нет соотвествующей поляризации')

        if not os.path.isdir(f"tmp\\transformed_image\\{tiff_file_name.upper()}"):
            os.mkdir(f"tmp\\transformed_image\\{tiff_file_name.upper()}")

        output_file_dir = os.path.join('tmp', 'transformed_image', f'{tiff_file_name.upper()}')
        tiff_file_basename = os.path.basename(tiff_file)
        output_file = os.path.join(output_file_dir, tiff_file_basename)
        shutil.copy(tiff_file, output_file)

        print('Files are selected')

        channel_stat = calculating_percentiles(output_file)
        print(f'Percentiles are calculated, {channel_stat}')

        qml_generator(channel_stat, output_file_dir)
        print(f'QML was generated')

        crop_tiff(output_file, boundary)
        print('Cropping is complete')

        os.remove(output_file)

    file_upload.file_upload(webgis_addr, webgis_username, webgis_password, 'tmp\\transformed_image', parent_id)

    # clear_directory('tmp\\transformed_image')
    # clear_directory('tmp')


def calculating_percentiles(tiff_file):
    dataset = gdal.Open(tiff_file)

    num_channels = dataset.RasterCount

    channel_stat = {}

    # for num_channel in range(1, num_channels + 1):
    channel = dataset.GetRasterBand(1)
    channel_data = channel.ReadAsArray()
    per_5 = np.percentile(channel_data, 5)
    per_95 = np.percentile(channel_data, 95)
    # channel_stat.append((os.path.basename(tiff_file), per_5, per_95))
    channel_stat = {
        'tiff_file': f"{os.path.basename(tiff_file).split('.')[0]}",
        'per_5': str(int(per_5)),
        'per_95': str(int(per_95)),
    }

    dataset = None

    return channel_stat


def qml_generator(channel_stat, folder):
    qml_tree = ET.parse('default_files\\default.qml')
    root = qml_tree.getroot()

    pipe = root.find('pipe')
    rasterrenderer = pipe.find('rasterrenderer')
    contrastEnhancement = rasterrenderer.find('contrastEnhancement')
    minValue = contrastEnhancement.find('minValue')
    maxValue = contrastEnhancement.find('maxValue')
    minValue.text = channel_stat['per_5']
    maxValue.text = channel_stat['per_95']

    # ET.indent(qml_tree, space='  ', level=0)

    return qml_tree.write(f"{folder}\\{channel_stat['tiff_file']}.qml", encoding='UTF-8')


def crop_tiff(input_file, boundary):
    crop_tiff = input_file.replace('.tiff', '_crop.tiff')

    ds = gdal.OpenEx(boundary)
    layer = ds.GetLayerByIndex(0)
    name = layer.GetName()

    res = gdal.Warp(
        crop_tiff,
        input_file,
        format='GTiff',
        cutlineDSName=boundary,
        cutlineLayer=name,
        cropToCutline=True
    )

    del res  # Вызываем деструктор, чтобы записать данные на диск


def reproject_geojson(input_file):
    source_crs = pyproj.CRS("EPSG:4326")
    target_crs = pyproj.CRS("EPSG:3857")

    project = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

    with open(input_file, 'r') as f:
        data = json.load(f)

    for feature in data['features']:
        geometry = feature['geometry']

        geometry['coordinates'] = project.transform(geometry['coordinates'][0][0], geometry['coordinates'][0][1])

    output_file = input_file.replace('.geojson', '_reprojected.geojson')
    with open(output_file, 'w') as f:
        json.dump(data, f)

    return output_file


def clear_directory(directory_path):
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} doesn't exist")
        return

    for item in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item)

        if os.path.isfile(item_path):
            os.remove(item_path)
            print(f"Delete file: {item_path}")
        elif os.path.isdir(item_path):
            clear_directory(item_path)
            os.rmdir(item_path)
            print(f"Delete directory: {item_path}")


# extract('images')

# webgis_addr = 'https://kolesnikov-p.nextgis.com'
# webgis_username = 'pvk200815@gmail.com'
# webgis_password = 'yNCY3VQ4zNDDYJ4'
# transform_tiff('images', 'boundary.geojson', webgis_addr, webgis_username, webgis_password, 57)
