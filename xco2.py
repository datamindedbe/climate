import datetime
import h5py


def xco2_to_dict(FILE_NAME):
    with h5py.File(FILE_NAME, mode='r') as f:
        # print "f.keys: %s" % f.keys()
        # print f['/RetrievalResults'].keys()
        dset_var = f['/RetrievalResults/xco2']
        dset_temp = f['/RetrievalResults/temperature_profile_ecmwf']
        dset_lat = f['/SoundingGeometry/sounding_latitude_geoid']
        dset_lon = f['/SoundingGeometry/sounding_longitude_geoid']
        dset_lev = f['/SoundingGeometry/sounding_altitude']
        dset_time = f['/RetrievalHeader/sounding_time_tai93']

        # Read the data.
        data = dset_var[:]
        temp = dset_temp[:]
        lat = dset_lat[:]
        lon = dset_lon[:]
        lev = dset_lev[:]
        time = [datetime.datetime(1993, 1, 1) + datetime.timedelta(seconds=t) for t in dset_time]
        #time = [datetime.datetime(t) for t in dset_time]
        dt = [t.strftime('%Y-%m-%d %H:%M:%S') for t in time[:]]

        # Read the needed attributes.
        data_units = dset_var.attrs['Units'][0]
        lev_units = dset_lev.attrs['Units'][0]

        # print data_units
        # print lev_units
        # print dset_var
        # print dset_lat
        # print dset_lon
        # print dset_time
        # print temp[0]

    result = [{'xco2':d.item(),
               'temperature': 0, #tmp.item(),
               "coordinates":{"coordinates":[lo.item(), l.item()],"type":"Point"},
               'lev': le.item(),
               'time': t}
              for d, tmp, l, lo, le, t in zip(data, temp, lat, lon, lev, time)]
    return result


def get_mapping():
    mapping = {
        "xco2": {
            "properties": {
                "time": {"type": "date", "format": "dateOptionalTime"},
                "xco2": {"type": "float"},
                "temperature": {"type": "float"},
                "lev": {"type": "float"},
                "coordinates": {
                    "type": "object",
                    "dynamic": "true",
                    "properties": {
                        "coordinates": {
                            "type": "geo_point",
                            # "lat_lon": "true",
                            "geohash": "true"
                        }
                    }
                }
            }
        }
    }
    return mapping

