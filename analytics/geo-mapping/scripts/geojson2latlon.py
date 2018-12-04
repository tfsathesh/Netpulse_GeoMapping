#!/usr/bin/env python

import pyproj, sys, json
from optparse import OptionParser

#if __name__ == '__main__':

bng = pyproj.Proj(init='epsg:27700')
wgs84 = pyproj.Proj(init='epsg:4326')

def traverse_list(l, reverse = False, to_gps = True):
    if isinstance(l[0], list) == False:
        x = float(l[0])
        y = float(l[1])

        x2, y2 = (x, y)
        if to_gps:
            x2, y2 = pyproj.transform(bng, wgs84, x, y)

        if reverse:
            return [y2,x2]
        else:
            return [x2, y2]
    else:
        l_new = []
        for el in l:
            l_new.append(traverse_list(el, reverse, to_gps))
        return l_new

def dump_geojson(data):
    out = '{\n'
    for key in data:
        if key != 'features':
            out += '"%s": "%s",\n' % (key, data[key])
    out += '"features": ['
    l = []
    for d in data['features']:
        s = '{\n'
        for key in filter(lambda x: x!='properties' and x!='geometry', d.keys()):
            s += '"%s": "%s",\n' % (key, d[key])
        s += '"properties": '
        s += json.dumps(d['properties'])
        s += ","
        s += '"geometry": '
        s += json.dumps(d['geometry'])
        s += '}'
        l.append(s)
    out += ',\n'.join(l)
    out += ']\n}'

    return out

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('', '--polygon-key', dest = 'POLYGON_KEY', default = None)
    parser.add_option('', '--no-to-gps', dest = 'TO_GPS', action = 'store_false', default = False)
    parser.add_option('', '--reverse',   dest = 'IS_REVERSE', action = 'store_true', default = True)
    opt, fnames = parser.parse_args()

    data = json.load(sys.stdin)

    for idx, feat_d in enumerate(data['features']):
        key = '-'
        if feat_d['properties'].has_key(opt.POLYGON_KEY):
            key = feat_d['properties'][opt.POLYGON_KEY]
        print >> sys.stderr, 'processing polygon:', idx, key

        feat_d['geometry']['coordinates'] = traverse_list(feat_d['geometry']['coordinates'], opt.IS_REVERSE, opt.TO_GPS)

    print dump_geojson(data)
