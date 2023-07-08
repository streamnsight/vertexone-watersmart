import logging
from typing import List
from typing import Optional
from sqlalchemy import String, Float, Integer, select, insert, update
from sqlalchemy.orm import DeclarativeBase, Session, Mapped, mapped_column
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from .parsers import parse_daily_measure, parse_hourly_measure, date_to_dt


logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass

class DailyMeasure(Base):
    __tablename__ = "santacruz_watersmart_daily"
    ts: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[str] = mapped_column(String(20))
    consumption: Mapped[float] = mapped_column(Float)
    precipitation: Mapped[Optional[float]]
    temperature: Mapped[Optional[float]]
    def __repr__(self) -> str:
        return f"DailyMeasure(ts={self.ts!r}, date={self.date!r}, consumption={self.consumption!r})"

class HourlyMeasure(Base):
    __tablename__ = "santacruz_watersmart_hourly"
    ts: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[str] = mapped_column(String(20))
    consumption: Mapped[float] = mapped_column(Float)
    leak: Mapped[Optional[float]]
    flags: Mapped[Optional[str]]
    def __repr__(self) -> str:
        return f"HourlyMeasure(ts={self.ts!r}, date={self.date!r}, consumption={self.consumption!r})"


class SQLStorageClass():

    def __init__(self, engine, insert=insert):
        self.engine = engine
        self.insert = insert
        self.type = type(self)
        self.daily_class = DailyMeasure
        self.hourly_class = HourlyMeasure
        self.daily_measure_parser = parse_daily_measure
        self.hourly_measure_parser = parse_hourly_measure
        with Session(engine) as session:
            Base.metadata.create_all(self.engine)

    def save(self, class_name, dataset):
        if class_name == self.daily_class:
            return self.save_daily(dataset)
        elif class_name == self.hourly_class:
            return self.save_hourly(dataset)

    def save_daily(self, dataset):
        with Session(self.engine) as session:
            for row in dataset:
                dt = date_to_dt(row['categories'])
                ts = int(dt.timestamp())
                date = dt.isoformat()
                values = {
                    "date": date, 
                    "consumption": row['consumption'], 
                    "temperature": row['temperature'], 
                    "precipitation": row['precipitation']
                }
                try:
                    query = self.insert(DailyMeasure).values(
                                    ts=ts, 
                                    **values
                                ).on_conflict_do_update(index_elements=DailyMeasure.__table__.primary_key, set_=values)
                    session.execute(query)
                except IntegrityError as e:
                    query = update(DailyMeasure).where(ts == ts).values(**values)
                    session.execute(query)
                except Exception as e:
                    logger.error(e)
            session.commit()            

    def save_hourly(self, dataset):
        with Session(self.engine) as session:
            for row in dataset:
                ts = row['ts']
                dt = datetime.fromtimestamp(ts)
                date = dt.isoformat()
                values = {
                    "date": date, 
                    "consumption": row['gallons'], 
                    "leak": row['leak_gallons'], 
                    "flags": "|".join(row['flags']) if row['flags'] is not None else None
                }
                try:
                    query = self.insert(HourlyMeasure).values(
                                    ts=ts, 
                                    **values
                                ).on_conflict_do_update(index_elements=HourlyMeasure.__table__.primary_key, set_=values)
                    session.execute(query)
                except IntegrityError as e:
                    query = update(HourlyMeasure).where(ts == ts).values(**values)
                    session.execute(query)
                except Exception as e:
                    logger.error(e)
            session.commit()

    def get_history(self, class_name, entity_parser, from_ts=None, to_ts=None, limit=None, offset=None, ascending=True):
        with Session(self.engine) as session:
            query = select(class_name)
            if from_ts and isinstance(from_ts, int):
                query = query.where(class_name.ts >= from_ts)
            if to_ts and isinstance(to_ts, int):
                query = query.where(class_name.ts <= to_ts)
            if ascending:
                query = query.order_by(class_name.ts.asc())
            else:
                query = query.order_by(class_name.ts.desc())
            if limit and isinstance(limit, int):
                query = query.limit(limit)
            if offset and isinstance(offset, int):
                query = query.offset(offset)
            dataset = []
            for row in session.execute(query):
                dataset.append(entity_parser(row[0]))
            return dataset
        
    @property
    def last_ts(self, class_name):
        with Session(self.engine) as session:
            query = select(class_name).order_by(class_name.ts.desc())
            logger.info(query)
            row = session.execute(query).first()
        if row:
            return row[0].ts
        else:
            return None
