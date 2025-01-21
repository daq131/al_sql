from sqlalchemy import Table, Column, Integer, String, MetaData, Float, ForeignKey
from sqlalchemy import create_engine, inspect, insert


engine = create_engine('sqlite:///database.db')
conn = engine.connect()
meta = MetaData()

stations_table = Table('stations', meta,
                       Column('id', Integer, primary_key=True),
                       Column('station', String),
                       Column('latitude', Float),
                       Column('longitude', Float),
                       Column('elevation', Float),
                       Column('name', String),
                       Column('country', String),
                       Column('state', String),)

measures_table = Table('measures', meta,
                       Column('id', Integer, primary_key=True),
                        Column('station_id', Integer, ForeignKey('stations.id'),),
                       Column('date', String),
                       Column('precip', Float),
                       Column('tobs', Integer),
                       )
# print(repr(meta.tables['stations']))
print(stations_table.columns.keys())
# print(measures_table.columns)
# print(stations_table.columns)
meta.create_all(engine)

ins = stations_table.insert().values(id = 1, station = 'USC00519397', latitude = 21.2716, longitude = -157.8168,
                                     elevation = 3.0, name = 'WAIKIKI 717.2', country ='US', state = 'HI')
mns = measures_table.insert().values(id = 1, station_id = 1, date = '2010-01-01', precip = 0.08, tobs = 65)

result = conn.execute(ins)
result2 = conn.execute(mns)

conn.execute(ins, [
    {'id': 2, 'station': 'USC00519397', 'latitude': 21.2716, 'longitude': -157.8168, 'elevation': 3, 'name': 'WAIKIKI 717.2', 'country': 'US', 'state': 'HI'},
    {'id': 3, 'station': 'USC00513117', 'latitude': 21.4234, 'longitude': -157.8015, 'elevation': 14.6, 'name': 'KANEOHE 838.1', 'country': 'US', 'state': 'HI'},
    {'id': 4, 'station': 'USC00514830', 'latitude': 21.5213, 'longitude': -157.8374, 'elevation': 7, 'name': 'KUALOA RANCH HEADQUARTERS 886.9', 'country': 'US', 'state': 'HI'},
    {'id': 5, 'station': 'USC00517948', 'latitude': 21.3934, 'longitude': -157.9751, 'elevation': 11.9, 'name': 'PEARL CITY', 'country': 'US', 'state': 'HI'},
    {'id': 6, 'station': 'USC00518838', 'latitude': 21.4992, 'longitude': -158.0111, 'elevation': 306.6, 'name': 'UPPER WAHIAWA 874.3', 'country': 'US','state':  'HI'},
    {'id': 7, 'station': 'USC00519523', 'latitude': 21.33556, 'longitude': -157.71139, 'elevation': 19.5, 'name': 'WAIMANALO EXPERIMENTAL FARM', 'country': 'US', 'state': 'HI'},
    {'id': 8, 'station': 'USC00519281', 'latitude': 21.45167, 'longitude': -157.84888999999998, 'elevation': 32.9, 'name': 'WAIHEE 837.5','country':  'US', 'state': 'HI'},
    {'id': 9, 'station': 'USC00511918', 'latitude': 21.3152, 'longitude': -157.9992, 'elevation': 0.9, 'name': 'HONOLULU OBSERVATORY 702.2', 'country': 'US', 'state': 'HI'},
    {'id': 10, 'station': 'USC00516128', 'latitude': 21.3331, 'longitude': -157.8025, 'elevation': 152.4, 'name': 'MANOA LYON ARBO 785.2', 'country': 'US', 'state': 'HI'}
])

conn.execute(mns, [
    {'id': 2, 'station_id': 1, 'date': '2010-01-01', 'precip': 0.08, 'tobs': 65},
    {'id': 3, 'station_id': 1, 'date': '2010-01-02', 'precip':  0.0, 'tobs': 63},
    {'id': 4, 'station_id': 1, 'date': '2010-01-03', 'precip': 0.0, 'tobs': 74},
    {'id': 5, 'station_id': 1, 'date': '2010-01-04', 'precip': 0.0, 'tobs': 76},
    {'id': 6, 'station_id': 1, 'date': '2010-01-06', 'precip': 0.0, 'tobs': 73},
    {'id': 7, 'station_id': 1, 'date': '2010-01-07', 'precip': 0.06, 'tobs': 70},
    {'id': 8, 'station_id': 1, 'date': '2010-01-08', 'precip': 0.0, 'tobs': 64},
    {'id': 9, 'station_id': 1, 'date': '2010-01-09', 'precip': 0.0, 'tobs': 68},
    {'id': 10, 'station_id': 1, 'date': '2010-01-10', 'precip': 0.0, 'tobs': 73}
])
#
s = stations_table.select().where(stations_table.c.id > 5)
results = conn.execute(s)
for row in results:
    print(row)
#
t = measures_table.select().where(measures_table.c.tobs > 65)
results2 = conn.execute(t)
for row in results2:
    print(row)

all = conn.execute(measures_table.select()).fetchall()
print(all)
query = stations_table.select()
print(query)
exe = conn.execute(query)
results = exe.fetchmany(5)
print(results)

# conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
# limit = conn.execute(measures_table.select()).limit(5)
# print(limit)
# output = conn.execute("SELECT * FROM stations_table")
# print(output.fetchall())