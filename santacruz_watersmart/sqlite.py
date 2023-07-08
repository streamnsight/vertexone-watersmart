from .storage_class import SQLStorageClass 
from sqlalchemy import create_engine
from sqlalchemy.dialects.sqlite import insert

DEFAULT_DB_NAME = 'scmu.db'


class SQLiteStorage(SQLStorageClass):

    db = DEFAULT_DB_NAME

    def __init__(self, db_name=DEFAULT_DB_NAME, echo=True):
        self.db = db_name
        self.engine = create_engine(f"sqlite:///{self.db}", echo=echo)
        super(SQLiteStorage, self).__init__(self.engine, insert=insert)
