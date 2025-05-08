import os

import requests

import helper_functions as h


def sites_marker_list():
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']

    url = ("https://deims.org/geoserver/deims/ows?service=WFS&version=1.0.0&"
           "request=GetFeature&typeName=deims:ilter_all_formal&outputFormat=application%2Fjson")

    r = requests.get(url)
    lter = r.json()
    datatable = []

    desc_dct = {i['properties']['name']: i['properties']['deimsid'] for i in lter['features'] if
                'agr' in i['properties']['name'].lower()}

    for station in lter['features']:
        lat = station['properties']['field_coordinates_lat']
        lon = station['properties']['field_coordinates_lon']

        deims_id = os.path.basename(station['properties']['deimsid'])
        site_url = f'https://deims.org/api/sites/{deims_id}'
        r = requests.get(site_url)
        site_info = r.json()

        sid = site_info['attributes']['general']['shortName']
        desc = site_info['attributes']['environmentalCharacteristics']['biome']

        try:
            country = site_info['attributes']['geographic']['country'][0]
            pi = site_info['attributes']['contact']['metadataProvider'][0]
        except TypeError:
            pi = ''
            country = ''

        if h.is_number(lat) and h.is_number(lon):
            l = ['lter']
            l += [float(lat)]
            l += [float(lon)]
            l += [country]
            l += [sid]
            l += [pi]
            l += [desc]

            assert len(l) == 7

            datatable.append(l)

    return datatable
