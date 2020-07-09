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

        # Address
        c.execute(''' CREATE TABLE IF NOT EXISTS address (
                        AddressID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
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
                        FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')

        # Code
        c.execute(''' CREATE TABLE IF NOT EXISTS code (
                        CodeID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        Category varchar(16) NOT NULL,
                        Code varchar(16) NOT NULL,
                        Language varchar(16) NOT NULL,
                        Description varchar(1024) NOT NULL
                        ); ''')

        # Contact
        c.execute(''' CREATE TABLE IF NOT EXISTS contact (
                        ContactID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        EntityNumber varchar(16) NOT NULL,
                        EntityContact varchar(8) NOT NULL,
                        ContactType varchar(8) NOT NULL,
                        Value varchar(64) NOT NULL,
                        FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')

        # Denomination
        c.execute(''' CREATE TABLE IF NOT EXISTS denomination (
                        DenominationID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        EntityNumber varchar(16) NOT NULL,
                        Language varchar(8) NOT NULL,
                        TypeOfDenomination varchar(8) NOT NULL,
                        Denomination varchar(512) NOT NULL,
                        FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')

        # Establishment
        c.execute(''' CREATE TABLE IF NOT EXISTS establishment (
                        EstablishmentID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        EstablishmentNumber varchar(16) NOT NULL,
                        StartDate Date,
                        EnterpriseNumber varchar(16) NOT NULL,
                        FOREIGN KEY (EnterpriseNumber) REFERENCES enterprise(EnterpriseNumber)
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
            with open("data/enterprise.csv", encoding="utf-8") as csv_file:
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
            conn.commit()
            print("Populated enterprise table")

        # Activity
        c.execute('SELECT COUNT(*) FROM activity')
        result = c.fetchone()
        if not result[0]:
            with open("data/activity.csv", encoding="utf-8") as csv_file:
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
                    if not line_count % 100 and line_count:
                        c.executemany('INSERT INTO activity(EntityNumber, ActivityGroup, NaceVersion, '
                                      'NaceCode, Classification) VALUES (?,?,?,?,?)', data)
                        del data[:]
                    line_count += 1
            conn.commit()
            print("Populated activity table")

        # Address
        c.execute('SELECT COUNT(*) FROM address')
        result = c.fetchone()
        if not result[0]:
            with open("data/address.csv", encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                data = []
                for row in csv_reader:
                    if line_count:
                        EntityNumber = row[0]
                        TypeOfAddress = row[1]
                        CountryNL = row[2]
                        CountryFR = row[3]
                        Zipcode = row[4]
                        MunicipalityNL = row[5]
                        MunicipalityFR = row[6]
                        StreetNL = row[7]
                        StreetFR = row[8]
                        HouseNumber = row[9]
                        Box = row[10]
                        ExtraAddressInfo = row[11]
                        DateStrikingOff = row[12]
                        data += [(EntityNumber, TypeOfAddress, CountryNL, CountryFR, Zipcode,
                                  MunicipalityNL, MunicipalityFR, StreetNL, StreetFR, HouseNumber,
                                  Box, ExtraAddressInfo, DateStrikingOff)]
                    # Inserts records in batches of 100
                    if not line_count % 100 and line_count:
                        c.executemany('INSERT INTO address(EntityNumber, TypeOfAddress, CountryNL, CountryFR, Zipcode,\
                                  MunicipalityNL, MunicipalityFR, StreetNL, StreetFR, HouseNumber,\
                                  Box, ExtraAddressInfo, DateStrikingOff) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', data)
                        del data[:]
                    line_count += 1
            conn.commit()
            print("Populated address table")

        # Code
        c.execute('SELECT COUNT(*) FROM code')
        result = c.fetchone()
        if not result[0]:
            with open("data/Code.csv", encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                data = []
                for row in csv_reader:
                    if line_count:
                        Category = row[0]
                        Code = row[1]
                        Language = row[2]
                        Description = row[3]
                        data += [(Category, Code, Language, Description)]
                    # Inserts records in batches of 100
                    if not line_count % 100 and line_count:
                        c.executemany('INSERT INTO code(Category, Code, Language, Description) VALUES (?,?,?,?)', data)
                        del data[:]
                    line_count += 1
            conn.commit()
            print("Populated code table")

        # Contact
        c.execute('SELECT COUNT(*) FROM contact')
        result = c.fetchone()
        if not result[0]:
            with open("data/Contact.csv", encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                data = []
                for row in csv_reader:
                    if line_count:
                        EntityNumber = row[0]
                        EntityContact = row[1]
                        ContactType = row[2]
                        Value = row[3]
                        data += [(EntityNumber, EntityContact, ContactType, Value)]
                    # Inserts records in batches of 100
                    if not line_count % 100 and line_count:
                        c.executemany('INSERT INTO contact(EntityNumber, EntityContact, ContactType, Value) VALUES (?,?,?,?)',
                                      data)
                        del data[:]
                    line_count += 1
            conn.commit()
            print("Populated contact table")

        # Denomination
        c.execute('SELECT COUNT(*) FROM denomination')
        result = c.fetchone()
        if not result[0]:
            with open("data/Denomination.csv", encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                data = []
                for row in csv_reader:
                    if line_count:
                        EntityNumber = row[0]
                        Language = row[1]
                        TypeOfDenomination = row[2]
                        Denomination = row[3]
                        data += [(EntityNumber, Language, TypeOfDenomination, Denomination)]
                    # Inserts records in batches of 100
                    if not line_count % 100 and line_count:
                        c.executemany('INSERT INTO denomination(EntityNumber, Language, TypeOfDenomination, Denomination) VALUES (?,?,?,?)',
                            data)
                        del data[:]
                    line_count += 1
            conn.commit()
            print("Populated denomination table")

        # Establishment
        c.execute('SELECT COUNT(*) FROM establishment')
        result = c.fetchone()
        if not result[0]:
            with open("data/Establishment.csv", encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")
                line_count = 0
                data = []
                for row in csv_reader:
                    if line_count:
                        EstablishmentNumber = row[0]
                        StartDate = row[1]
                        EnterpriseNumber = row[2]
                        data += [(EstablishmentNumber, StartDate, EnterpriseNumber)]
                    # Inserts records in batches of 100
                    if not line_count % 100 and line_count:
                        c.executemany(
                            'INSERT INTO establishment(EstablishmentNumber, EnterpriseNumber, EnterpriseNumber) VALUES (?,?,?)',
                            data)
                        del data[:]
                    line_count += 1
            conn.commit()
            print("Populated establishment table")

        conn.commit()
        conn.close()
