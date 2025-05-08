# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 2018
Last change on Nov  1 2018
@author: Claudio D'Onofrio
"""

__version__= "0.1.0"

# create helper functions
#-----------------------------------------------------------------


def is_number(num):
    """ check if we deal with a number """
    try:
        float(num)
        return True
    except ValueError:
        return False

#-----------------------------------------------------------------
 
def debugPrint(dbg, msg):
    """ expect true|false and a message. if "true", print msg """
    if(dbg):
        print(msg)
        
#-----------------------------------------------------------------

def checklib(module):
    """ load a list of modoules if available, otherwise throw exception """
    import imp
    for mod in module:
        try:
            imp.find_module(mod)
            ret = 1
        except ImportError as imperror:
            print(imperror)
            ret = 0
    return ret

#---------------------------------------------------------------------
def getIcon(network=None, theme=None, size=32, returnLnk=False):
    import folium
    from folium.features import CustomIcon
    icon = None
    base = 'https://claudiodonofrio.github.io/res/icons/'    
    if network == 'icos':
        icoLnk = base+'icos_ico.png'
        icon = CustomIcon(icoLnk, icon_size=(size,size))        
        if theme=='AS':
            icoLnk = base+'atc_ico.png'
            icon = CustomIcon(icoLnk, icon_size=(size,size))
        if theme=='ES':
            icoLnk = base+'etc_ico.png'
            icon = CustomIcon(icoLnk, icon_size=(size,size))
        if theme=='OS':
            icoLnk = base+'otc_ico.png'
            icon = CustomIcon(icoLnk, icon_size=(size,size))
    if network == 'neon':
        icoLnk = base+'neon_ico.png'
        icon = CustomIcon(icoLnk, icon_size=(size,size))
    if network == 'ameriflux':
        icoLnk = base+'ameriflux_ico.png'
        icon = CustomIcon(icoLnk, icon_size=(size,size))
    if network == 'asiaflux':
        icoLnk = base+'asiaflux_ico.png'
        icon = CustomIcon(icoLnk, icon_size=(size,size))
    if network == 'lter':
        icoLnk = base+'deims_ico.png'
        icon = CustomIcon(icoLnk, icon_size=(size,size))
    if network == 'fluxnet':
        icoLnk = base+'fluxnet_ico.png'
        icon = CustomIcon(icoLnk, icon_size=(size,size))
    
    if icon is None:        
        icon=folium.Icon(color='blue',icon='info-sign')
    if returnLnk:
        lnk = "<img src='" + icoLnk + "' height='16' width='16'>"
        return lnk
    return icon


#---------------------------------------------------------------------
def icos_stations(*args):
    """
        Define SPARQL query to get a list of ICOS stations
        If %args is empty, all stations are returned.
        If %args = 4, we expect a bounding box, [lat1, lat2, lon1, lon2]
        If %args is >0 and != 4, we revert to return all stations
    """

    if len(args) != 4:
        filterstr = " "
    else:
        filterstr = """
            filter(
            ?lat >= %s && ?lat <= %s &&
            ?lon >= %s && ?lon <= %s)."""  % (args)


    query = """
        PREFIX cpst: <http://meta.icos-cp.eu/ontologies/stationentry/>
        SELECT
        (IF(bound(?lat), str(?lat), "?") AS ?latstr)
        (IF(bound(?lon), str(?lon), "?") AS ?lonstr)
        (REPLACE(str(?class),"http://meta.icos-cp.eu/ontologies/stationentry/", "") AS ?themeShort)
        (str(?country) AS ?Country)
        (str(?sName) AS ?Short_name)
        (str(?lName) AS ?Long_name)
        (GROUP_CONCAT(?piLname; separator=";") AS ?PI_names)
        (str(?siteType) AS ?Site_type)
        FROM <http://meta.icos-cp.eu/resources/stationentry/>
        WHERE {
        ?s cpst:hasCountry ?country .
        ?s cpst:hasShortName ?sName .
        ?s cpst:hasLongName ?lName .
        ?s cpst:hasSiteType ?siteType .
        ?s cpst:hasPi ?pi .
        ?pi cpst:hasLastName ?piLname .
        ?s a ?class .
        OPTIONAL{?s cpst:hasLat ?lat } .
        OPTIONAL{?s cpst:hasLon ?lon } .
        OPTIONAL{?s cpst:hasSpatialReference ?spatRef } .
        OPTIONAL{?pi cpst:hasFirstName ?piFname } .
        %s
        }
        GROUP BY ?lat ?lon ?class ?country ?sName ?lName ?siteType
        ORDER BY ?themeShort ?sName
        """ %filterstr

    return query
#------------------------------------------------------------------------------
