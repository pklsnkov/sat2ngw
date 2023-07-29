from NGSatSearch.NGSatSearch import NGSatSearch
import os
import zipfile
import json
from osgeo import gdal, ogr, osr
import pyproj

import numpy as np

import xml.etree.ElementTree as ET


def extract(folder):
    dir_list = os.listdir(folder)
    for directory in dir_list:
        dir_path = os.path.join(folder, directory)
        with zipfile.ZipFile(dir_path, 'r') as zip_open:
            zip_open.extractall(folder)
        os.remove(dir_path)
    print('Распаковка успешна')


def transform_tiff(folder, boundary, polarization_type=None):

    if not os.path.isdir('transformed_image'):
        os.mkdir('transformed_image')

    for attribute_folder in os.listdir(folder):
        measurement_folder = os.path.join(folder, attribute_folder, 'measurement')
        if polarization_type is None:
            tiff_file = os.path.join(measurement_folder, os.listdir(measurement_folder)[0])
            tiff_file_name, tiff_file_ext = os.path.splitext(os.path.basename(tiff_file))
        else:
            if polarization_type == 'vh':
                for file in os.listdir(measurement_folder):
                    if '_vh_' in file:
                        tiff_file = os.path.join(measurement_folder, file)

                        tiff_file_name, tiff_file_ext = os.path.splitext(os.path.basename(tiff_file))
            elif polarization_type == 'vv':
                for file in os.listdir(measurement_folder):
                    if '_vv_' in file:
                        tiff_file = os.path.join(measurement_folder, file)
                        tiff_file_name, tiff_file_ext = os.path.splitext(os.path.basename(tiff_file))

        if not os.path.isdir(f"transformed_image\\{tiff_file_name.upper()}"):
            os.mkdir(f"transformed_image\\{tiff_file_name.upper()}")

        output_file = os.path.join('transformed_image', f'{tiff_file_name.upper()}', f'{tiff_file_name}_reproj.tiff')

        print('Файлы выбраны')

        source_dataset = gdal.Open(tiff_file)
        target_projection = 'EPSG:3857'

        if source_dataset is None:
            raise Exception("Не удалось открыть исходный GeoTIFF.")

        src_srs = source_dataset.GetGCPProjection()
        gcps = source_dataset.GetGCPs()

        translate_options = gdal.TranslateOptions(outputSRS=target_projection, GCPs=gcps)

        gdal.Translate(output_file, source_dataset, options=translate_options)

        # tiff_files.append(output_file)

        source_dataset = None

        print('Перепроецирование завершено')

        # boundary_rep = reproject_geojson(boundary)
        #
        # cut_tiff = crop_tiff(output_file, boundary_rep, f"transformed_image\\{tiff_file_name.upper()}")

        channel_stat = calculating_percentiles(output_file)
        print(channel_stat)

        # os.remove(cut_tiff)

        qml_generator(channel_stat, f"transformed_image\\{tiff_file_name.upper()}")

    # return output_file


def calculating_percentiles(tiff_file):
    dataset = gdal.Open(tiff_file)

    num_channels = dataset.RasterCount

    channel_stat = {}

    for num_channel in range(1, num_channels + 1):
        channel = dataset.GetRasterBand(num_channel)
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


def crop_tiff(input_file, boundary, folder):
    cut_tiff = os.path.join(folder, 'raster.tiff')

    # gdal.SetConfigOption('CHECK_DISK_FREE_SPACE', 'FALSE')

    ds = gdal.OpenEx(boundary)
    layer = ds.GetLayerByIndex(0)
    name = layer.GetName()

    # ds_tiff = gdal.Open(input_file)

    res = gdal.Warp(
        cut_tiff,  # Результирующий обрезанный растр
        input_file,  # Исходный ратср
        format='GTiff',  # Формат выходного растра
        cutlineDSName=boundary,  # Путь до векторного набора с маской
        cutlineLayer=name,  # Имя слоя внутри векторного набора, пояснения ниже отдельно
        cropToCutline=True
        # Указание, что нужно не просто сбросить все пиксели вне маски в NoData, но и обрезать экстент растра по экстенту маски)
    )

    del res  # Вызываем деструктор, чтобы записать данные на диск

    return cut_tiff


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


# extract('images')
# transform_tiff('images', 'boundary.geojson')
