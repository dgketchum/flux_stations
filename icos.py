import os

import numpy as np
import pandas as pd
import json
import requests
import helper_functions as h


def sites_marker_list(icos_meta_file='icos_meta.json'):
    """Data portal: https://data.icos-cp.eu/portal/"""
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc', 'start', 'end']
    datatable = []

    with open(icos_meta_file, 'r') as f:
        icos_metadata = json.load(f)

    url = 'https://meta.icos-cp.eu/sparql'
    r = requests.get(url, params={'format': 'json', 'query': h.icos_stations()}, timeout=10)
    r.raise_for_status()
    data = r.json()

    for row in data['results']['bindings']:
        l = ['icos']
        site_id = row['Short_name']['value']

        latitude_str = row.get('latstr', {}).get('value')
        longitude_str = row.get('lonstr', {}).get('value')
        try:
            latitude = float(latitude_str) if latitude_str else None
            longitude = float(longitude_str) if longitude_str else None
        except ValueError:
            print(f'ICOS coords at {site_id}: lat: {latitude_str}; lon: {longitude_str}')
            continue

        site_info = icos_metadata.get(site_id, {})
        start_date = site_info.get('start_date', '')
        end_date = site_info.get('end_date', '')

        l += [latitude]
        l += [longitude]
        l += [row['Country']['value']]
        l += [site_id]
        l += [row['PI_names']['value']]
        l += [row['Site_type']['value']]
        l += [start_date]
        l += [end_date]

        assert len(l) == 9
        datatable.append(l)

    return datatable


def extract_icos_metadata(data_root, output_file):
    metadata = {}
    for site_dir in os.listdir(data_root):
        site_path = os.path.join(data_root, site_dir)
        if os.path.isdir(site_path) and site_dir.startswith("FLX_"):
            site_id = site_dir.split('_')[1]
            start_date = None
            end_date = None
            for file in os.listdir(site_path):
                if file.endswith(".csv") and site_id in file and 'FULLSET_DD' in file:
                    file_path = os.path.join(site_path, file)
                    print(file)
                    df = pd.read_csv(file_path, index_col='TIMESTAMP', parse_dates=True)
                    df = df[['LE_F_MDS']]
                    df[df['LE_F_MDS'] < -9998] = np.nan
                    df = df.dropna(subset=['LE_F_MDS'])

                    first_ts = df.index[0]
                    last_ts = df.index[-1]
                    start_date = first_ts.strftime(format='%Y-%m-%d')
                    end_date = last_ts.strftime(format='%Y-%m-%d')

                    break

            metadata[site_id] = {'start_date': start_date, 'end_date': end_date}
    with open(output_file, 'w') as f:
        json.dump(metadata, f, indent=4)


if __name__ == '__main__':
    icos_root_dir = '/home/dgketchum/data/IrrigationGIS/climate/icos'
    icos_metadata_file = 'icos_meta.json'
    extract_icos_metadata(icos_root_dir, icos_metadata_file)

# ========================= EOF ============================================================================
