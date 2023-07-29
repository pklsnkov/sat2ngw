from NGSatSearch.NGSatSearch import NGSatSearch
import os
import zipfile
from osgeo import gdal, osr

import numpy as np

import xml.etree.ElementTree as ET


def extract(folder):
    file_list = os.listdir(folder)
    for file in file_list:
        filepath = os.path.join(folder, file)
        with zipfile.ZipFile(filepath, 'r') as zip_open:
            zip_open.extractall(folder)
        os.remove(filepath)


def transform_tiff(folder, polarization_type=None):
    tiff_files = []

    if not os.path.isdir('images_reproj'):
        os.mkdir('images_reproj')

    for attribute_folder in os.listdir(folder):
        measurement_folder = os.path.join(folder, attribute_folder, 'measurement')
        if polarization_type is None:
            tiff_file = os.path.join(measurement_folder, os.listdir(measurement_folder)[0])

            tiff_file_name, tiff_file_ext = os.path.splitext(os.path.basename(tiff_file))

            if not os.path.isdir(f"images_reproj\\{tiff_file_name}"):
                os.mkdir(f"images_reproj\\{tiff_file_name}")

            output_file = os.path.join('images_reproj', f'{tiff_file_name}', f'{tiff_file_name}_reproj{tiff_file_ext}')
        else:
            if polarization_type == 'vh':
                pass  # todo : заглушки
            elif polarization_type == 'vv':
                pass

        src_dataset = gdal.Open(tiff_file)
        target_projection = 'EPSG:3857'

        if src_dataset is None:
            raise Exception("Не удалось открыть исходный GeoTIFF.")

        src_srs = src_dataset.GetGCPProjection()
        gcps = src_dataset.GetGCPs()

        translate_options = gdal.TranslateOptions(outputSRS=target_projection, GCPs=gcps)

        gdal.Translate(output_file, src_dataset, options=translate_options)

        # tiff_files.append(output_file)

        channel_stat = calculating_percentiles(output_file)
        print(channel_stat)

        qml_generator(channel_stat, f"images_reproj\\{tiff_file_name}")

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
            'tiff_file': f'{os.path.splitext(os.path.basename(tiff_file))[0]}',
            'per_5': str(per_5).split('.')[0],
            'per_95': str(per_95).split('.')[0],
        }

    return channel_stat

def qml_generator(channel_stat, folder):
    # for stat in channel_stat:

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




# transform_tiff('images - Copy')
# output_file = reproject_and_calculating_percentiles('images - Copy')
# calculating_percentiles(output_file)
