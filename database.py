import sqlalchemy
import logging

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

logging.getLogger().setLevel(logging.INFO)


def check_version():
    logging.info(sqlalchemy.__version__)

def connect(url):
    engine = create_engine(url, echo=True)
    return engine

def test_create_table():
    url = 'sqlite:///data/sqlalchemy.db'
    engine = connect(url)
    meta = MetaData()

    students = Table(
        'students', meta,
        Column('id', Integer, primary_key = True),
        Column('name', String),
        Column('lastname', String),
    )

    meta.create_all(engine)

    logging.info(engine)


def main():
    test_create_table()

if __name__ == '__main__':
    main()