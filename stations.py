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

networks = ['lter','ozflux', 'asiaflux', 'fluxnet', 'ameriflux', 'neon',  'icos']


def get_stations(shp):
    master_list = []
    for n in networks:
        sublist = eval(n + '.sites_marker_list()')
        print(n)
        master_list.extend(sublist)

    data_ = np.array(master_list)
    df = pd.DataFrame(data=data_, columns=['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc'])

    df.columns = ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']
    df = df[['network', 'country', 'sid', 'pi', 'desc', 'lat', 'lon']]
    df.to_csv(shp.replace('.shp', '.csv'))

    gdf = gpd.GeoDataFrame(df, geometry=[Point(r['lon'], r['lat']) for i, r in df.iterrows()])

    gdf.to_file(shp)

    print(f'wrote {shp}')


if __name__ == '__main__':
    home = '/media/research'
    d = os.path.join(home, 'IrrigationGIS', 'climate')
    shp_ = os.path.join(d, 'flux_stations_21APR2025.shp')
    get_stations(shp_)

# ========================= EOF ============================================================================
