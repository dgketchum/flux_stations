import requests
import folium
from folium.plugins import MarkerCluster
import textile
from bs4 import BeautifulSoup
import helper_functions as h


def sites_marker_list():
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']

    url = 'http://asiaflux.net/index.php?'
    url = url + 'action=multidatabase_view_main_init&visible_item=1000&multidatabase_id=8&block_id=2346'

    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    tables = soup.find_all("table", {"class": "mdb_metadata_table"})

    asiaflux = []
    for t in tables:
        output_rows = []
        for table_row in t.findAll('tr'):
            detail = []
            for column in table_row:
                detail.append(column.text)
            output_rows.append(detail)
        asiaflux.append(output_rows)

    datatable = []

    for station in asiaflux:
        lat = float(station[2][1])
        lon = float(station[3][1])

        l = ['asiaflux']
        l += [lat]
        l += [lon]
        l += ['']
        l += [station[0][1]]
        l += ['']
        l += [station[1][1]]

        assert len(l) == 7

        datatable.append(l)

    return datatable
