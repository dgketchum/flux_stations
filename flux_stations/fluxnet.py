import csv

def sites_marker_list():
    """ Export from https://ameriflux.lbl.gov/sites/site-search/"""
    # ['network', 'lat', 'lon', 'country', 'sid', 'pi', 'desc']

    _file = 'Fluxnet_sites.csv'
    datatable = []
    with open(_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        _ = next(reader)
        for row in reader:
            l = ['fluxnet']
            l += [float(row[4])]
            l += [float(row[5])]
            l += ['']
            l += [row[0]]
            l += ['']
            l += [row[1]]

            assert len(l) == 7

            datatable.append(l)

    return datatable



