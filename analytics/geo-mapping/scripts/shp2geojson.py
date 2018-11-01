import shapefile, json
from optparse import OptionParser

if __name__ == '__main__':
    parser = OptionParser()
    opt, fnames = parser.parse_args()

    # read the shapefile
    reader = shapefile.Reader(fnames[0])
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    buffer = []
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        buffer.append(dict(type="Feature", geometry=geom, properties=atr)) 

    # write the GeoJSON file

    print json.dumps({"type": "FeatureCollection", "features": buffer})
