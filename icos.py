import helper_functions as h

import requests


def sites_marker_list():
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']

    url = 'https://meta.icos-cp.eu/sparql'
    r = requests.get(url, params={
        'format': 'json',
        'query': h.icos_stations()})

    data = r.json()

    datatable = []

    for row in data['results']['bindings']:
        l = ['icos']

        try:
            l += [float(row['latstr']['value'])]
            l += [float(row['lonstr']['value'])]
        except ValueError:
            continue

        l += [row['Country']['value']]
        l += [row['Short_name']['value']]
        l += [row['PI_names']['value']]
        l += [row['Long_name']['value']]

        assert len(l) == 7

        datatable.append(l)

    return datatable
