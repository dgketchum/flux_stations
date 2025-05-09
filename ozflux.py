import csv
import json
import os
import re
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
import xarray as xr
from requests.exceptions import HTTPError, ChunkedEncodingError
from thefuzz import process, fuzz


def sites_marker_list(metadata_file='ozflux_meta.json', sites_file='ozflux_sites.csv'):
    """"""
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']

    datatable = []

    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    with open(sites_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        for row in reader:
            site_id = row[1]
            site_info = metadata.get(site_id, {})
            latitude = site_info.get('latitude')
            longitude = site_info.get('longitude')
            start_date = site_info.get('start_date', '')
            end_date = site_info.get('end_date', '')
            l = [
                'ozflux',
                latitude,
                longitude,
                '',
                row[1],
                row[7],
                row[2],
                start_date,
                end_date
            ]
            assert len(l) == 9
            datatable.append(l)
    return datatable


def download_latest_site_files(dest, records_csv, sites_csv, overwrite=False):
    urls_df = pd.read_csv(records_csv)
    sites_df = pd.read_csv(sites_csv)

    extracted_data = urls_df['Title'].str.extract(r'(\d{4})_v(\d+)$', expand=True)
    urls_df['Year'] = pd.to_numeric(extracted_data[0], errors='coerce')
    urls_df['Version'] = pd.to_numeric(extracted_data[1], errors='coerce')

    valid_urls_df = urls_df.dropna(subset=['Year', 'Version']).copy()
    valid_urls_df['Year'] = valid_urls_df['Year'].astype(int)
    valid_urls_df['Version'] = valid_urls_df['Version'].astype(int)

    title_choices = valid_urls_df['Title']

    for site_row in sites_df.itertuples(index=False):
        site_name = site_row.Name
        fluxnet_id = site_row.Fluxnet

        if pd.isna(fluxnet_id):
            fluxnet_id = site_name.replace(' ', '')

        nc_filename = os.path.join(dest, f'{fluxnet_id}_daily.nc')
        csv_filename = nc_filename.replace('.nc', '.csv')

        if os.path.exists(nc_filename) and os.path.exists(csv_filename) and not overwrite:
            print(f'{fluxnet_id} exists, skipping')
            continue

        potential_matches = process.extract(site_name, title_choices, scorer=fuzz.token_sort_ratio, limit=5)
        if potential_matches:
            match_indices = [item[2] for item in potential_matches]
            fuzzy_matches_df = valid_urls_df.loc[match_indices]
        else:
            print(f'No matches for {site_name}')
            continue

        latest_row = fuzzy_matches_df.sort_values(by=['Year', 'Version'], ascending=[False, False]).iloc[0]

        catalog_link = latest_row.get('Access Data link').split(' | ')[0]

        parsed_catalog_link = urlparse(catalog_link)
        path_match = re.match(r'(.*/catalog/)(.+/)([^/]+)/([^/]+)/catalog\.html', parsed_catalog_link.path,
                              re.IGNORECASE)

        base_url = f"{parsed_catalog_link.scheme}://{parsed_catalog_link.netloc}"
        path_prefix = path_match.group(1).replace('/catalog/', '/fileServer/')
        site_path_segment = path_match.group(2)
        site_name_in_url = path_match.group(3)
        year_version_in_url = path_match.group(4)

        file_level = 'L6'
        file_freq = 'Daily'

        possible_names = [site_name.replace(' ', ''), site_name_in_url.replace(' ', '')]

        valid_url = False
        for _name in possible_names:
            file_path = f"{path_prefix}{site_path_segment}{site_name_in_url}/{year_version_in_url}/{file_level}/default/{_name}_{file_level}_{file_freq}.nc"
            download_url = urljoin(base_url, file_path)
            try:
                response = requests.get(download_url, stream=True)
                response.raise_for_status()
                valid_url = True
                break
            except HTTPError:
                continue

        if not valid_url:
            continue

        try:
            with open(nc_filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        except ChunkedEncodingError:
            print(f"{site_name} failed during download")
            continue

        print(f"Downloaded: {site_name} ({fluxnet_id}) from {download_url} -> {nc_filename}")

        ds = xr.open_dataset(nc_filename)
        df = ds.to_dataframe()
        ds.close()

        df.to_csv(csv_filename)
        print(f"Converted and saved: {nc_filename} -> {csv_filename}")


def extract_ozflux_metadata(data_dir, output_file):
    metadata = {}
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            print(filename)
            site_id = filename.split('_')[0]
            filepath = os.path.join(data_dir, filename)
            try:
                df = pd.read_csv(filepath)
                if 'time' in df.columns and 'latitude' in df.columns and 'longitude' in df.columns:
                    start_date = df['time'].iloc[0].split(' ')[0]
                    end_date = df['time'].iloc[-1].split(' ')[0]
                    latitude = df['latitude'].iloc[0]
                    longitude = df['longitude'].iloc[0]
                    metadata[site_id] = {
                        'latitude': latitude,
                        'longitude': longitude,
                        'start_date': start_date,
                        'end_date': end_date
                    }
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    with open(output_file, 'w') as f:
        json.dump(metadata, f, indent=4)


if __name__ == '__main__':
    home = '/media/research'
    if not os.path.isdir(home):
        home = '/home/dgketchum/data'

    d = os.path.join(home, 'IrrigationGIS', 'climate')
    shp_ = os.path.join(d, 'flux_stations_18APR2025.shp')
    dst = os.path.join(d, 'ozflux')
    # download_latest_site_files(dst, records_csv='ozflux_catalog.csv', sites_csv='ozflux_sites.csv', overwrite=False)

    output_filename = 'ozflux_meta.json'
    extract_ozflux_metadata(dst, output_filename)
    print(f"Metadata written to {output_filename}")

# ========================= EOF ============================================================================
