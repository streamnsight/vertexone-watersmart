import logging
import os

import httpx

from .exceptions import (NotAuthenticatedException, ServerErrorException,
                         UnauthorizedException, UnknownException,
                         UnknownProviderException)
from .parsers import (parse_daily_dataset, parse_daily_measure,
                      parse_hourly_dataset, parse_hourly_measure)
from .providers import PROVIDER_LIST

ENV_VARIABLE_PREFIX = "WS"
DAILY_DELAY_DAYS = 1
HOURLY_DELAY_HOURS = 3

logger = logging.getLogger(__name__)


class BaseClient:
    last_hourly_ts = None
    last_daily_ts = None
    hourly_dataset = None
    daily_dataset = None
    db = None

    def __init__(self, provider, db=None):
        if db is not None:
            self.db = db
        known_providers = [k for k, v in PROVIDER_LIST.items()]
        if provider not in known_providers:
            raise UnknownProviderException(provider)
        self._setup_provider(provider)

    def _setup_provider(self, provider):
        self.provider = provider
        self.root_url = f"https://{provider}.watersmart.com/index.php"
        self.session_url = f"{self.root_url}/logout"
        self.login_url = f"{self.root_url}/logout/login?forceEmail=1"
        self.hourly_data_url = f"{self.root_url}/rest/v1/Chart/RealTimeChart"
        self.daily_data_url = f"{self.root_url}/rest/v1/Chart/weatherConsumptionChart?module=portal&commentary=full"
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Referrer": f"{self.root_url}/home/index",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        }

    def _check_credentials(self, username=None, password=None):
        if username is not None:
            self.username = username
        else:
            # try getting from env
            self.username = os.environ.get(f"{ENV_VARIABLE_PREFIX}_USERNAME")
        if password is not None:
            self.password = password
        else:
            # try getting from env
            self.password = os.environ.get(f"{ENV_VARIABLE_PREFIX}_PASSWORD")

    def _check_status_code(self, req):
        if req.status_code == 401:
            raise NotAuthenticatedException()
        elif req.status_code == 403:
            raise UnauthorizedException()
        elif req.status_code == 500:
            raise ServerErrorException()
        elif req.status_code == 200:
            return
        else:
            raise UnknownException(
                f"Request failed with status code: {req.status_code}"
            )

    def set_daily(self):
        if self.db is not None:
            self.class_name = self.db.daily_class
        self.input_parser = parse_daily_dataset
        self.output_parser = parse_daily_measure
        self.data_url = self.daily_data_url
        return self

    def set_hourly(self):
        if self.db is not None:
            self.class_name = self.db.hourly_class
        self.input_parser = parse_hourly_dataset
        self.output_parser = parse_hourly_measure
        self.data_url = self.hourly_data_url
        return self


class SyncClient(BaseClient):
    def __init__(
        self,
        provider,
        db=None,
    ):
        self._last_ts = None
        super(SyncClient, self).__init__(provider, db)

    # common methods
    def _process_data(self, data):
        data = self.input_parser(data)
        if self.db:
            self.db.save(self.class_name, data)
            self._last_ts = data[-1]["ts"]
            return self.get_history()
        else:
            return data

    def _merge_cookies(self, jar, req):
        # grab cookies from the redirected urls responses
        for res in req.history:
            for cookie in res.cookies.jar:
                jar.set_cookie(cookie)
        return jar

    def get_history(
        self, from_ts=None, to_ts=None, limit=None, offset=None, ascending=True
    ):
        if self.db is not None:
            return self.db.get_history(
                self.class_name,
                self.output_parser,
                from_ts,
                to_ts,
                limit,
                offset,
                ascending,
            )
        else:
            raise NotImplementedError("no storage backend defined")

    @property
    def last_ts(self):
        if self._last_ts is None and self.db:
            result = self.get_history(limit=1, ascending=False)
            if len(result) > 0:
                self._last_ts = result[0]["ts"]
        return self._last_ts

    # sync methods
    def _get_session_cookies(self):
        with httpx.Client(follow_redirects=True) as client:
            req = client.get(url=self.session_url)
            self._check_status_code(req)
            return req.cookies.jar

    def login(self, username, password):
        self._check_credentials(username, password)
        self.jar = self._get_session_cookies()
        # login
        with httpx.Client(follow_redirects=True) as client:
            payload = {"email": self.username, "password": self.password}
            req = client.post(
                url=self.login_url, headers=self.headers, data=payload, cookies=self.jar
            )
            self._check_status_code(req)
            self.jar = self._merge_cookies(self.jar, req)
            return self.jar

    def fetch(self):
        jar = self.login(self.username, self.password)
        with httpx.Client(follow_redirects=True) as client:
            req = client.get(url=self.data_url, headers=self.headers, cookies=jar)
            self._check_status_code(req)
            return self._process_data(req.json())


class AsyncClient(SyncClient):
    def __init__(
        self,
        provider,
        db=None,
    ):
        super(AsyncClient, self).__init__(provider, db)

    # async methods
    async def _get_session_cookies(self):
        async with httpx.AsyncClient(follow_redirects=True) as client:
            req = await client.get(url=self.session_url)
            self._check_status_code(req)
            return req.cookies.jar

    async def login(self, username, password):
        self._check_credentials(username, password)
        self.jar = await self._get_session_cookies()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            # login
            payload = {"email": self.username, "password": self.password}
            req = await client.post(
                url=self.login_url, headers=self.headers, data=payload, cookies=self.jar
            )
            self._check_status_code(req)
            self.jar = self._merge_cookies(self.jar, req)
            return self.jar

    async def fetch(self):
        jar = await self.login(self.username, self.password)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            req = await client.get(url=self.data_url, headers=self.headers, cookies=jar)
            self._check_status_code(req)
            return self._process_data(req.json())


class Client:
    def __init__(self, provider, storage_engine=None, is_async=False):
        if is_async:
            self.client = AsyncClient(provider, storage_engine)
            self.login = self.client.login
        else:
            self.client = SyncClient(provider, storage_engine)
            self.login = self.client.login

    @property
    def daily(self):
        self.client.set_daily()
        return self.client

    @property
    def hourly(self):
        self.client.set_hourly()
        return self.client
