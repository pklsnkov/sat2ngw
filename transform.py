from NGSatSearch.NGSatSearch import NGSatSearch
import os
import shutil
import zipfile
import geojson
from shapely.geometry import shape
from osgeo import gdal, ogr, osr
import pyproj
import json

import numpy as np

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
            except:
                os.remove(dir_path)
    print('Распаковка успешна')  # todo : добавить обработку плохих зипов (ниже ошибка)


'''
raise BadZipFile("File is not a zip file")
zipfile.BadZipFile: File is not a zip file
'''


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

        output_file_dir = os.path.join('transformed_image', f'{tiff_file_name.upper()}')
        tiff_file_basename = os.path.basename(tiff_file)
        output_file = os.path.join(output_file_dir, tiff_file_basename)
        shutil.copy(tiff_file, output_file)

        print('Файлы выбраны')

        # src_dataset = gdal.Open(tiff_file)
        # target_projection = 'EPSG:3857'
        #
        # if src_dataset is None:
        #     raise Exception("Не удалось открыть исходный GeoTIFF.")
        #
        # # src_srs = src_dataset.GetGCPProjection()
        # gcps = src_dataset.GetGCPs()
        #
        # src_width = src_dataset.RasterXSize
        # src_height = src_dataset.RasterYSize
        #
        # translate_options = gdal.TranslateOptions(outputSRS=target_projection,
        #                                           GCPs=gcps,
        #                                           width=src_width,
        #                                           height=src_height,
        #                                           )
        #
        # gdal.Translate(output_file,
        #                src_dataset,
        #                options=translate_options)

        # src_width = src_dataset.RasterXSize
        # src_height = src_dataset.RasterYSize
        #
        # warp_options = gdal.WarpOptions(dstSRS=target_projection,
        #                                 format='GTiff',
        #                                 srcSRS='EPSG:4326',
        #                                 width=src_width,
        #                                 height=src_height
        #                                 )
        #
        # gdal.Warp(output_file,
        #           src_dataset,
        #           options=warp_options)

        # tiff_files.append(output_file)
        #
        # source_dataset = None
        #
        # print('Перепроецирование завершено')

        channel_stat = calculating_percentiles(output_file)
        print(channel_stat)

        qml_generator(channel_stat, f"transformed_image\\{tiff_file_name.upper()}")

        crop_tiff(output_file, boundary)
        print('Обрезка завершена')

        os.remove(output_file)

    # return output_file


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
    cut_tiff = input_file.replace('.tiff', '_cut.tiff')

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


extract('images')
# transform_tiff('images - Copy', 'boundary.geojson')


# def geojson_to_wkt(boundary):
#     with open(boundary, 'r') as f:
#         data = geojson.load(f)
#
#     wkt_geometries = []
#
#     if data['type'] == 'FeatureCollection':
#         for feature in data['features']:
#             geometry = feature['geometry']
#             wkt_geometries.append(shape(geometry).wkt)
#     else:
#         geometry = data['geometry']
#         wkt_geometries.append(shape(geometry).wkt)
#
#     return '\n'.join(wkt_geometries)
#
# wkt_geometries = geojson_to_wkt('boundary.geojson')
#
# print(wkt_geometries)
