from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker

import sqlite3

engine = create_engine("sqlite:///ingested.db", echo=False)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

conn = sqlite3.connect('ingested.db',check_same_thread=False)
cur = conn.cursor()

class Header(Base):
    __tablename__ = 'headers'
    header_id = Column(Integer, primary_key=True, autoincrement=True)
    file_type = Column(String)
    company_id = Column(String)
    file_created_date = Column(String)
    file_created_time = Column(String)
    file_generation_number = Column(String)

    def __repr__(self):
        return '\n' + f"'{self.header_id}','{self.file_type}','{self.company_id}','{self.file_created_date}','{self.file_created_time}','{self.file_generation_number}'"

class Consumption(Base):
    __tablename__ = 'consumptions'
    consumption_id = Column(Integer, primary_key=True, autoincrement=True)
    file_generation_number_fk = Column(String)
    meter_number = Column(Integer)
    measurement_date = Column(String)
    measurement_time = Column(String)
    consumption = Column(Float)

    def __repr__(self):
       return '\n' + f"'{self.consumption_id}','{self.file_generation_number_fk}','{self.meter_number}','{self.measurement_date}','{self.measurement_time}',{self.consumption}"

try:
    # drop tables
    Header.__table__.drop(engine)
    Consumption.__table__.drop(engine)

except Exception as e:
    print(e)

finally:
    # create all tables
    Base.metadata.create_all(engine)

class Inserter:
    def __init__(self,lst_headers,lst_consumptions):
        self.lst_headers = lst_headers
        self.lst_consumptions = lst_consumptions

    def insert(self):
        # inserts into headers and consumptions if file_generation_number not exists in the headers table...
        if not bool([i for i in session.query(Header).filter_by(file_generation_number=self.lst_headers[-1])]):
            cur.executemany('INSERT INTO headers(file_type, company_id, file_created_date, file_created_time, file_generation_number) VALUES (?, ?, ?, ?, ?)', (self.lst_headers,))
            cur.executemany('INSERT INTO consumptions(file_generation_number_fk, meter_number, measurement_date, measurement_time, consumption) VALUES (?, ?, ?, ?, ?)', self.lst_consumptions)
            conn.commit()
            print(f"1 record inserted into table: ingested.headers")
            print(f"{len(self.lst_consumptions)} record(s) inserted into table: ingested.consumptions")

        else:
            print("File already exists in ingested.headers - ignoring inserts")