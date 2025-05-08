import requests

import helper_functions as h


def sites_marker_list():
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']

    url = "http://data.neonscience.org/api/v0/locations/sites"

    r = requests.get(url)
    neonsites = r.json()
    datatable = []

    for station in neonsites['data']:
        if 'locationDecimalLatitude' in station and 'locationDecimalLongitude' in station:
            if h.is_number(station['locationDecimalLatitude']) and h.is_number(station['locationDecimalLongitude']):
                l = ['neon']
                l += [float(station['locationDecimalLatitude'])]
                l += [float(station['locationDecimalLongitude'])]
                l += ['']
                l += [station['locationName']]
                l += ['']
                l += [station['locationDescription']]

                assert len(l) == 7

                datatable.append(l)

    return datatable
