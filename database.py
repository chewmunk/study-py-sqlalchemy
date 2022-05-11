import sqlalchemy
import logging

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

logging.getLogger().setLevel(logging.INFO)


class DataBase:

    def __init__(self, url):
        self.engine = create_engine(url, echo=True)

    def connect(self):
        pass
        
        

    



def check_version():
    logging.info(sqlalchemy.__version__)

def get_engine(url):
    engine = create_engine(url, echo=True)
    return engine

def connect():
    url = 'sqlite:///data/sqlalchemy.db'
    engine = get_engine(url)
    conn = engine.connect()
    return conn

def test_create_table():
    url = 'sqlite:///data/sqlalchemy.db'
    engine = get_engine(url)
    meta = MetaData() # Meta object that will hold this table

    students = Table(
        'students', meta,
        Column('id', Integer, primary_key = True),
        Column('name', String),
        Column('lastname', String),
    )

    meta.create_all(engine)

    logging.info(engine)

def test_insert_data_in_table():
    url = 'sqlite:///data/sqlalchemy.db'
    engine = get_engine(url)
    meta = MetaData() 

    students = Table(
        'students', meta,
        Column('id', Integer, primary_key = True),
        Column('name', String),
        Column('lastname', String),
    )   

    ins = students.insert()
    ins = students.insert().values(name = 'Ravi', lastname = 'Kappor')
    conn = engine.connect()
    result = conn.execute(ins)

    logging.info(result.inserted_primary_key)

def test_insert_multiple_in_table():
    url = 'sqlite:///data/sqlalchemy.db'
    engine = get_engine(url)
    conn = engine.connect()
    meta = MetaData()

    students = Table(
        'students', meta,
        Column('id', Integer, primary_key = True),
        Column('name', String),
        Column('lastname', String),
    )   
    result = conn.execute(students.insert(), [
        {'name':'Rajiv', 'lastname' : 'Khanna'},
        {'name':'Komal','lastname' : 'Bhandari'},
        {'name':'Abdul','lastname' : 'Sattar'},
        {'name':'Priya','lastname' : 'Rajhans'},
    ])

    logging.info(result)

def test_select_table():
    conn = connect()
    meta = MetaData()

    students = Table(
        'students', meta,
        Column('id', Integer, primary_key = True),
        Column('name', String),
        Column('lastname', String),
    )   

    s = students.select() # 'SELECT students.id, students.name, students.lastname FROM students'
    result = conn.execute(s)
    for row in result:
        logging.info(row)


    s = students.select().where(students.c.id > 2) #  c attribute is an alias for column.

    result = conn.execute(s)
    for s in result:
        logging.info(s)

def test_textual_sql():
    from sqlalchemy import text
    t = text('SELECT * FROM students')
    conn = connect()
    result = conn.execute(t)

    for row in result:
        logging.info(row)

    # select students.name, students.lastname from students where students.name between ? and ?
    # s = text("select students.name, students.lastname from students where students.name between :x and :y")
    # The values of x = ’A’ and y = ’L’ are passed as parameters. Result is a list of rows with names between ‘A’ and ‘L’ −
    # result = conn.execute(s, x = 'A', y = 'L').fetchall() # parameter를 넣을 수 있음 


    # for row in result:
        # logging.info(row)

    from sqlalchemy import text
    stmt = text("SELECT * FROM students WHERE students.name BETWEEN :x AND :y")
    result = conn.execute(stmt, {"x": "A", "y": "L"})

    for row in result:
        logging.info(row)

    from sqlalchemy.sql import select
    s = select([text("students.name, students.lastname from students")]).where(text("students.name between :x and :y"))
    result = conn.execute(s, x = 'A', y = 'L').fetchall()

    for row in result:
        logging.info(row) 

    from sqlalchemy import and_
    from sqlalchemy import select

    s = select([text('* from students')])\
        .where(
            and_(
                text('students.name between :x and :y'),
                text('students.id>2')
            )
        )
    result = conn.execute(s, {"x": "A", "y": "L"}).fetchall()

    for row in result:
        logging.info(row)

def main():
    # test_create_table()
    # test_insert_data_in_table()
    # test_insert_multiple_in_table()
    # test_select_table()
    test_textual_sql()

if __name__ == '__main__':
    main()