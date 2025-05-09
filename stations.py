import os
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point

import icos
import neon
import asiaflux
import fluxnet
import ameriflux
import lter
import ozflux

networks = ['ameriflux', 'icos', 'ozflux']


# networks = ['ozflux',  'ameriflux', 'icos', 'lter', 'asiaflux', 'fluxnet', 'neon']


def get_stations(shp):
    master_list = []
    for n in networks:
        sublist = eval(n + '.sites_marker_list()')
        print(n)
        master_list.extend(sublist)

    data_ = np.array(master_list)
    df = pd.DataFrame(data=data_, columns=['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc', 'start', 'end'])

    df.columns = ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc', 'start', 'end']
    df = df[['network', 'country', 'sid', 'pi', 'desc', 'lat', 'lon', 'start', 'end']]
    df.to_csv(shp.replace('.shp', '.csv'))

    df.dropna(inplace=True)

    gdf = gpd.GeoDataFrame(df, geometry=[Point(r['lon'], r['lat']) for i, r in df.iterrows()])

    gdf.to_file(shp)

    print(f'wrote {shp}')


if __name__ == '__main__':
    home = '/home/dgketchum/data'
    d = os.path.join(home, 'IrrigationGIS', 'climate', 'flux_stations')
    shp_ = os.path.join(d, 'flux_stations_08MAY2025.shp')
    get_stations(shp_)

# ========================= EOF ============================================================================
