import os

from vertexone_watersmart.client import Client
from vertexone_watersmart.sqlite import SQLiteStorage

username = os.environ.get('V1WS_USERNAME')
password = os.environ.get('V1WS_PASSWORD')

# optional, instantiate a backend store
db = SQLiteStorage('scmu.db', echo=False) # set echo=False to turn logs off
# instantiate the client, choosing a known provider
scmu = Client(provider='santacruz', storage_engine=db)
scmu.login(username=username, password=password)
# fetch latest dataset
daily_data = scmu.daily.fetch()
print(daily_data[:10])
hourly_data = scmu.hourly.fetch()
print(hourly_data[:10])

# get history (all params are optional, from and to are unix timestamps)
daily_data = scmu.daily.get_history(from_ts=1685494800, to_ts=1686974400, limit=10, offset=0, ascending=True)
print(daily_data[:10])
hourly_data = scmu.hourly.get_history(from_ts=1685494800, to_ts=1686974400, limit=10, offset=0, ascending=True)
print(hourly_data[:10])
