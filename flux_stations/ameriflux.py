import csv
import zipfile
import os
import geopandas as gpd


def sites_marker_list():
    """ Export from https://ameriflux.lbl.gov/sites/site-search/"""
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']

    _file = 'AmeriFlux-sites.csv'
    datatable = []
    with open(_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        for row in reader:
            l = ['ameriflux']
            l += [float(row[13])]
            l += [float(row[14])]
            l += [row[12]]
            l += [row[0]]
            l += [row[2]]
            l += [row[1]]

            assert len(l) == 7

            datatable.append(l)

    return datatable


def unzip_ameriflux_by_site_id(zip_dir, shapefile, sid_col):
    gdf = gpd.read_file(shapefile)
    site_ids_in_shapefile = set(gdf[sid_col])
    unzipped_count = 0
    for filename in os.listdir(zip_dir):
        if filename.endswith(".zip"):
            site_id_match = filename.split('_')[1] if len(filename.split('_')) > 1 else None
            if site_id_match in site_ids_in_shapefile:
                zip_path = os.path.join(zip_dir, filename)
                extract_dir = os.path.join(zip_dir, filename[:-4])
                os.makedirs(extract_dir, exist_ok=True)
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    print(f"Unzipped: {filename} to {extract_dir}")
                    unzipped_count += 1
                except zipfile.BadZipFile:
                    print(f"Warning: Could not unzip {filename}. It seems to be a bad zip file.")
    print(f"\nUnzipped {unzipped_count} files.")


if __name__ == '__main__':
    zip_directory = '/media/research/IrrigationGIS/climate/ameriflux/amf_new'
    shapefile_path = ('/home/dgketchum/PycharmProjects/swim-rs/tutorials/6_Flux_International/'
                      'data/gis/6_Flux_International_crops_8MAY2025.shp')
    site_id_column = 'sid'

    unzip_ameriflux_by_site_id(zip_directory, shapefile_path, site_id_column)
