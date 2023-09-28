import asyncio
import os
from datetime import datetime

from vertexone_watersmart.client import Client
from vertexone_watersmart.sqlite import SQLiteStorage


async def main():
    username = os.environ.get('V1WS_USERNAME')
    password = os.environ.get('V1WS_PASSWORD')

    db = SQLiteStorage('scmu.db', echo=True) # set echo=False to turn logs off
    scmu = Client(provider='santacruz', storage_engine=db, is_async=True)
    
    scmu.login(username=username, password=password)
    # fetch latest dataset
    [daily_data, hourly_data] = await asyncio.gather(*[
        scmu.daily.fetch(),
        scmu.hourly.fetch()
    ])
    print(daily_data[:10])
    print(hourly_data[:10])

    # get history (all params are optional, from and to are unix timestamps)
    daily_data = scmu.daily.get_history(from_ts=1685494800, to_ts=1686974400, limit=10, offset=0, ascending=True)
    print(daily_data[:10])
    hourly_data = scmu.hourly.get_history(from_ts=1685494800, to_ts=1686974400, limit=10, offset=0, ascending=True)
    print(hourly_data[:10])

asyncio.run(main())
