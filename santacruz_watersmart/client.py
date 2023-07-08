import httpx
import os
import logging
from datetime import datetime, timedelta
from .parsers import parse_daily_dataset, parse_hourly_dataset

ROOT_URL = "https://santacruz.watersmart.com/index.php"
SESSION_URL = f"{ROOT_URL}/logout"
LOGIN_URL = f"{ROOT_URL}/logout/login?forceEmail=1"
HOURLY_DATA_URL = f"{ROOT_URL}/rest/v1/Chart/RealTimeChart"
DAILY_DATA_URL = f"{ROOT_URL}/rest/v1/Chart/weatherConsumptionChart?module=portal&commentary=full"
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Referrer": "https://santacruz.watersmart.com/index.php/home/index",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}
ENV_VARIABLE_PREFIX = 'SCMU'
DAILY_DELAY_DAYS = 1
HOURLY_DELAY_HOURS = 3

logger = logging.getLogger(__name__)


class BaseClient():
    
    last_hourly_ts = None
    last_daily_ts = None
    hourly_dataset = None
    daily_dataset = None
    db = None

    def __init__(self, username=None, password=None, db=None):
        if db is not None:
            self.db = db

        if username is not None:
            self.username = username
        else:
            # try getting from env
            self.username = os.environ.get(f'{ENV_VARIABLE_PREFIX}_USERNAME')
        if password is not None:
            self.password = password
        else:
            # try getting from env
            self.password = os.environ.get(f'{ENV_VARIABLE_PREFIX}_PASSWORD')
        
    def _check_status_code(self, req):
        if req.status_code == 401:
            raise Exception('Not authenticated')
        elif req.status_code == 403:
            raise Exception('Unauthorized')
        elif req.status_code == 500:
            raise Exception('Server Error')
        elif req.status_code == 200:
            return 
        else:
            raise Exception(f'Request failed with status code: {req.status_code}')
        

class Client(BaseClient):

    def __init__(self, data_url, class_name, input_parser, output_parser, username=None, password=None, db=None):
        self.class_name = class_name
        self.data_url = data_url
        self.input_parser = input_parser
        self.output_parser = output_parser
        self._last_ts = None
        super(Client, self).__init__(username, password, db)
    
    # common methods
    def _process_data(self, data):
        data = self.input_parser(data)
        if self.db:
            self.db.save(self.class_name, data)
            self.last_ts = data[-1]['ts']
            return self.get_history()
        else:
            return data

    def _merge_cookies(self, jar, req):
        # grab cookies from the redirected urls responses
        for res in req.history:
            for cookie in res.cookies.jar:
                jar.set_cookie(cookie)
        return jar

    def get_history(self, from_ts=None, to_ts=None, limit=None, offset=None, ascending=True):
        if self.db is not None:
            return self.db.get_history(self.class_name, self.output_parser, from_ts, to_ts, limit, offset, ascending)
        else:
            raise NotImplementedError('no storage backend defined')
        
    @property
    def last_ts(self): 
        if self._last_ts is None and self.db:
            result = self.get_history(limit=1, ascending=False)
            if len(result) > 0:
                self._last_ts = result[0]['ts']
        return self._last_ts

    # sync methods
    def _get_session_cookies(self):
        with httpx.Client(follow_redirects=True) as client:
            req = client.get(url=SESSION_URL)
            self._check_status_code(req)
            return req.cookies.jar
    
    def _get_auth_cookies(self):
        jar = self._get_session_cookies()    
        # login
        with httpx.Client(follow_redirects=True) as client:
            payload = {"email": self.username, "password": self.password}
            req = client.post(url=LOGIN_URL, headers=HEADERS, data=payload, cookies=jar)
            self._check_status_code(req)
            return self._merge_cookies(jar, req)

    def fetch(self):
        jar = self._get_auth_cookies()
        with httpx.Client(follow_redirects=True) as client:
            req = client.get(url=self.data_url, headers=HEADERS, cookies=jar)
            self._check_status_code(req)
            return self._process_data(req.json())


class AsyncClient(Client):

    def __init__(self, data_url, class_name, input_parser, output_parser, username=None, password=None, db=None):
        super(AsyncClient, self).__init__(data_url, class_name, input_parser, output_parser, username, password, db)

    # async methods
    async def _get_session_cookies(self):
        async with httpx.AsyncClient(follow_redirects=True) as client:
            req = await client.get(url=SESSION_URL)
            self._check_status_code(req)
            return req.cookies.jar

    async def _get_auth_cookies(self):
        jar = await self._get_session_cookies()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            # login
            payload = {"email": self.username, "password": self.password}
            req = await client.post(url=LOGIN_URL, headers=HEADERS, data=payload, cookies=jar)
            self._check_status_code(req)
            return self._merge_cookies(jar, req)
        
    async def fetch(self):
        jar = await self._get_auth_cookies()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            req = await client.get(url=self.data_url, headers=HEADERS, cookies=jar)
            self._check_status_code(req)
            return self._process_data(req.json())
    

class SCMU():

    def __init__(self, username, password, storage_engine=None, is_async=False):
        daily_class = None
        daily_output_parser = None
        hourly_class = None
        hourly_output_parser = None
        if storage_engine is not None:
            daily_class = storage_engine.daily_class
            hourly_class = storage_engine.hourly_class
            daily_output_parser = storage_engine.daily_measure_parser
            hourly_output_parser = storage_engine.hourly_measure_parser
        if is_async:
            self.daily = AsyncClient(DAILY_DATA_URL, daily_class, parse_daily_dataset, daily_output_parser, username, password, storage_engine)
            self.hourly = AsyncClient(HOURLY_DATA_URL, hourly_class, parse_hourly_dataset, hourly_output_parser, username, password, storage_engine)
        else:
            self.daily = Client(DAILY_DATA_URL, daily_class, parse_daily_dataset, daily_output_parser, username, password, storage_engine)
            self.hourly = Client(HOURLY_DATA_URL, hourly_class, parse_hourly_dataset, hourly_output_parser, username, password, storage_engine)
        
