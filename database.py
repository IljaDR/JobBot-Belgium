import sqlite3
import csv
import os.path
from os import path

DatabaseLocation = "database/companies.db"


def check_if_data_exists():
    return path.exists("data/activity.csv")


def remove_quotes():
    return

class Database:
    def __init__(self):
        if not path.exists("database"):
            os.mkdir("database")

    def create_tables(self):
        # TODO: replace this with error
        if not check_if_data_exists():
            return

        conn = sqlite3.connect(DatabaseLocation)

        c = conn.cursor()

        # Checks if table exists
        # c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='enterprise' ''')
        # if c.fetchone()[0] == 0:

        # Enterprise
        c.execute(''' CREATE TABLE IF NOT EXISTS enterprise (
                        EnterpriseNumber varchar(16) NOT NULL,
                        Status varchar(8) NOT NULL,
                        JuridicalSituation varchar(8) NOT NULL,
                        TypeOfEnterprise varchar(2) NOT NULL,
                        JuridicalForm varchar(4),
                        StartDate Date NOT NULL,
                        PRIMARY KEY (EnterpriseNumber)
                        ); ''')

        # Activity
        c.execute(''' CREATE TABLE IF NOT EXISTS activity (
                        ActivityID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        EntityNumber varchar(16) NOT NULL,
                        ActivityGroup varchar(8) NOT NULL,
                        NaceVersion varchar(8) NOT NULL,
                        NaceCode varchar(8) NOT NULL,
                        Classification varchar(4),
                        FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')

        # Code
        c.execute(''' CREATE TABLE IF NOT EXISTS code (
                        Category varchar(16) NOT NULL,
                        Code varchar(16) NOT NULL,
                        Language varchar(16) NOT NULL,
                        Description varchar(1024) NOT NULL,
                        PRIMARY KEY (Category)
                        ); ''')

        # Address
        c.execute(''' CREATE TABLE IF NOT EXISTS address (
                        EntityNumber varchar(16) NOT NULL,
                        TypeOfAddress varchar(8) NOT NULL,
                        CountryNL varchar(64),
                        CountryFR varchar(64),
                        Zipcode varchar(8),
                        MunicipalityNL varchar(32),
                        MunicipalityFR varchar(32),
                        StreetNL varchar(64),
                        StreetFR varchar(64),
                        HouseNumber varchar(8),
                        Box varchar(8),
                        ExtraAddressInfo varchar(8),
                        DateStrikingOff varchar(8),
                        PRIMARY KEY (EntityNumber),
                        FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')

        # Establishment
        c.execute(''' CREATE TABLE IF NOT EXISTS establishment (
                        EstablishmentNumber varchar(16) NOT NULL,
                        StartDate Date NOT NULL,
                        EnterpriseNumber varchar(16) NOT NULL,
                        PRIMARY KEY (EnterpriseNumber),
                        FOREIGN KEY (EnterpriseNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')

        # Denomination
        c.execute(''' CREATE TABLE IF NOT EXISTS denomination (
                        EntityNumber varchar(16) NOT NULL,
                        ActivityGroup varchar(8) NOT NULL,
                        NaceVersion varchar(8) NOT NULL,
                        NaceCode varchar(8) NOT NULL,
                        Classification varchar(4),
                        PRIMARY KEY (EntityNumber),
                        FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')

        # Contact
        c.execute(''' CREATE TABLE IF NOT EXISTS contact (
                                EntityNumber varchar(16) NOT NULL,
                                EntityContact varchar(8) NOT NULL,
                                ContactType varchar(8) NOT NULL,
                                Value varchar(64) NOT NULL,
                                PRIMARY KEY (EntityNumber),
                                FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                                ); ''')
        print("Created tables if they didn't exist")

        conn.commit()
        conn.close()

    def populate_tables(self):
        # TODO: replace this with error
        if not check_if_data_exists():
            return

        conn = sqlite3.connect(DatabaseLocation)
        c = conn.cursor()

        # Enterprise
        # Only populate table if it is empty
        c.execute('SELECT COUNT(*) FROM enterprise')
        result = c.fetchone()
        if not result[0]:
            with open("data/enterprise.csv") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                data = []
                for row in csv_reader:
                    if line_count:
                        EnterpriseNumber = row[0]
                        Status = row[1]
                        JuridicalSituation = row[2]
                        TypeOfEnterprise = row[3]
                        JuridicalForm = row[4]
                        StartDate = row[5]
                        data += [(EnterpriseNumber, Status, JuridicalSituation, TypeOfEnterprise, JuridicalForm, StartDate)]

                    # Inserts records in batches of 100
                    if not line_count % 100 and line_count:
                        c.executemany('INSERT INTO enterprise VALUES (?,?,?,?,?,?)', data)
                        del data[:]
                    line_count += 1
            print("Populated enterprise table")

        # Activity
        c.execute('SELECT COUNT(*) FROM activity')
        result = c.fetchone()
        if not result[0]:
            with open("data/activity.csv") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                data = []
                for row in csv_reader:
                    if line_count:
                        EntityNumber = row[0]
                        ActivityGroup = row[1]
                        NaceVersion = row[2]
                        NaceCode = row[3]
                        Classification = row[4]
                        data += [(EntityNumber, ActivityGroup, NaceVersion, NaceCode, Classification)]

                    # Inserts records in batches of 100
                    if not line_count % 1 and line_count:
                        c.executemany('INSERT INTO activity(EntityNumber, ActivityGroup, NaceVersion, NaceCode, Classification) VALUES (?,?,?,?,?)', data)
                        #conn.commit()
                        del data[:]
                    line_count += 1
            print("Populated activity table")

        conn.commit()
        conn.close()
