import csv
import json
import os
import zipfile
from datetime import datetime

import geopandas as gpd
import pandas as pd


def sites_marker_list(ameriflux_meta_file='ameriflux_meta.json'):
    """ Export from https://ameriflux.lbl.gov/sites/site-search/"""
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc', 'start', 'end']

    datatable = []

    with open(ameriflux_meta_file, 'r') as f:
        ameriflux_metadata = json.load(f)

    with open('AmeriFlux-sites.csv', mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        for row in reader:
            network = 'ozflux' if row[1].startswith('AU-') else 'ameriflux'
            site_id = row[0]
            metadata = ameriflux_metadata.get(site_id)

            if metadata:
                latitude = metadata.get('latitude')
                longitude = metadata.get('longitude')
                start_date = metadata.get('start_date', '')
                end_date = metadata.get('end_date', '')

            else:
                latitude = float(row[13])
                longitude = float(row[14])
                start_date = None
                end_date = None

            l = [
                network,
                latitude,
                longitude,
                '',
                site_id,
                row[7],
                row[2],
                start_date,
                end_date
            ]
            assert len(l) == 9
            datatable.append(l)

    return datatable


def unzip_ameriflux_by_site_id(zip_dir, shapefile, sid_col):
    gdf = gpd.read_file(shapefile)
    site_ids_in_shapefile = set(gdf[sid_col])
    unzipped_count = 0
    for filename in os.listdir(zip_dir):
        if filename.endswith(".zip"):
            extract_dir = os.path.join(zip_dir, filename[:-4])
            site_id_match = filename.split('_')[1] if len(filename.split('_')) > 1 else None
            if site_id_match in site_ids_in_shapefile:
                zip_path = os.path.join(zip_dir, filename)
                os.makedirs(extract_dir, exist_ok=True)
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    print(f"Unzipped: {filename} to {extract_dir}")
                    unzipped_count += 1
                except zipfile.BadZipFile:
                    print(f"Warning: Could not unzip {filename}. It seems to be a bad zip file.")
    print(f"\nUnzipped {unzipped_count} files.")


def format_ameriflux_date(date_string):
    if date_string is not None and len(str(date_string)) >= 8:
        try:
            date_object = datetime.strptime(str(int(float(date_string))), '%Y%m%d%H%M')
            return date_object.strftime('%Y-%m-%d')
        except ValueError:
            try:
                return datetime.strptime(date_string, '%Y%m%d').strftime('%Y-%m-%d')
            except ValueError:
                return None
    return None


def extract_ameriflux_metadata(data_root, output_file):
    metadata = {}
    for site_dir in os.listdir(data_root):
        site_path = os.path.join(data_root, site_dir)
        if os.path.isdir(site_path) and site_dir.startswith("AMF_") and not site_dir.endswith('.zip'):
            site_id = site_dir.split('_')[1]
            for file in os.listdir(site_path):
                if file.endswith(".xlsx") and site_id in file:
                    file_path = os.path.join(site_path, file)
                    try:
                        print(file)
                        df = pd.read_excel(file_path)
                        meta_dict = df.set_index('VARIABLE').to_dict()['DATAVALUE']
                        latitude = meta_dict.get('LOCATION_LAT', None)
                        longitude = meta_dict.get('LOCATION_LONG', None)

                        start_date_raw = meta_dict.get('FLUX_MEASUREMENTS_DATE_START', None)
                        start_date_str = format_ameriflux_date(start_date_raw)

                        end_date_raw = meta_dict.get('FLUX_MEASUREMENTS_DATE_END', None)
                        end_date_str = format_ameriflux_date(end_date_raw)

                        if latitude is not None:
                            try:
                                latitude = float(latitude)
                            except ValueError:
                                latitude = None
                        if longitude is not None:
                            try:
                                longitude = float(longitude)
                            except ValueError:
                                longitude = None

                        metadata[site_id] = {
                            'latitude': latitude,
                            'longitude': longitude,
                            'start_date': start_date_str if start_date_str else None,
                            'end_date': end_date_str if end_date_str else None
                        }
                        break
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
                    break

    with open(output_file, 'w') as f:
        json.dump(metadata, f, indent=4)


if __name__ == '__main__':
    d = '/media/research/IrrigationGIS'
    p = '/home/dgketchum/PycharmProjects/swim-rs/tutorials'
    if not os.path.exists(d):
        d = '/home/dgketchum/data/IrrigationGIS'
        p = '/data/ssd2/swim'

    zip_directory = os.path.join(d, 'climate/ameriflux/amf_new')
    shapefile_path = os.path.join(p, '6_Flux_International/data/gis/6_Flux_International_crops_8MAY2025.shp')
    site_id_column = 'sid'

    # unzip_ameriflux_by_site_id(zip_directory, shapefile_path, site_id_column)

    ameriflux_metadata_file = 'ameriflux_meta.json'
    extract_ameriflux_metadata(zip_directory, ameriflux_metadata_file)
    print(f"AmeriFlux metadata written to {ameriflux_metadata_file}")

# ========================= EOF ============================================================================
