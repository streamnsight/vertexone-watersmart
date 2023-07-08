# Santa Cruz WaterSmart (Santa Cruz Municipal Utilities) data module

This project aims at making data from the Santa Cruz Municipal Utilies website available for programatic use.

There is no REST API per say, despite the naming of the url, but there are 2 useful data endpoints used to render charts on the site:

- Daily consumption, which includes daily weather reference data.
- Hourly consumption (in gallons), with some 'leak' flag, although it never seems to be set.

There are several caveats to using these endpoints:

- The datasets available are datasets meant to render the charts in the website, therefore they include a range of data, not just the latest datapoint.

- The data sent to the front-end is always the full dataset, and there is no range query parameters that I could find. (Note that the hourly dataset seems to be covering over a year's worth of data, and the daily more than 3 years so it is not clear how the 'start' point is defined; It is probably when the meter was upgraded).

- Most importantly, the **hourly** data is only available about twice a day, so it is not 'real-time' per say. I have seen the data be updated up to 9:00AM, 12:00PM as well as 12:00AM, so it is not clear what the schedule is.

- Sometimes, the weather data is not available, but it will show later.

## Functionality

The module provides a way to fetch the latest dataset for both hourly and daily consumption. This can be used as-is, but the user will need to manage it's own resulting datastream (i.e. merge overlapping ranges from sequential fetches, and/or filter by time range)

To facilitate extraction of a non-overlapping dataset, as well as range queries, an optional local storage mechanism is provided (leveraging SQLAlchemy and SQLite for now, but expandable to other SQLAlchemy backends). It merges the datasets each time it they are fetched, and allow for retrieval of historical data by time range.

## Install

```bash
pip install santacruz-watersmart
```

## Usage

### Synchronous Client

```python
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
```

### Async Client

To support modern use cases, the client support async requests by simply setting the parameter `async=True` on creation, and then using the async/await syntax.

```python
import asyncio
import os
from santacruz_watersmart.sqlite import SQLiteStorage
from santacruz_watersmart.client import SCMU
from datetime import datetime


async def main():
    username = os.environ.get('SCMU_USERNAME')
    password = os.environ.get('SCMU_PASSWORD')

    db = SQLiteStorage('scmu.db', echo=True) # set echo=False to turn logs off
    scmu = SCMU(storage_engine=db, username=username, password=password, is_async=True)
    
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
```