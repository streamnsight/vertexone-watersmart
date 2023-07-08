import os
from santacruz_watersmart.sqlite import SQLiteStorage
from santacruz_watersmart.client import SCMU

username = os.environ.get('SCMU_USERNAME')
password = os.environ.get('SCMU_PASSWORD')

# optional, instantiate a backend store
db = SQLiteStorage('scmu.db', echo=True) # set echo=False to turn logs off
# instantiate the client
scmu = SCMU(storage_engine=db, username=username, password=password)

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
