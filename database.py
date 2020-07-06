import sqlite3


class Database:
    def create_tables(self):
        conn = sqlite3.connect('database/companies.db')

        c = conn.cursor()

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='enterprise' ''')
        if c.fetchone()[0] == 0:
            c.execute(''' CREATE TABLE enterprise (
                EnterpriseNumber varchar(16) NOT NULL,
                Status varchar(8) NOT NULL,
                JuridicalSituation varchar(8) NOT NULL,
                TypeOfEnterprise varchar(2) NOT NULL,
                JuridicalForm varchar(4),
                StartDate Date NOT NULL,
                PRIMARY KEY (EnterpriseNumber)
                ); ''')
            print('Created enterprise table"')

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='activity' ''')
        if c.fetchone()[0] == 0:
            c.execute(''' CREATE TABLE activity (
                        EntityNumber varchar(16) NOT NULL,
                        ActivityGroup varchar(8) NOT NULL,
                        NaceVersion varchar(8) NOT NULL,
                        NaceCode varchar(8) NOT NULL,
                        Classification varchar(4),
                        PRIMARY KEY (EntityNumber),
                        FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                        ); ''')
            print('Created activity table')

        #Update SQL \/
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='code' ''')
        if c.fetchone()[0] == 0:
            c.execute(''' CREATE TABLE code (
                                Category varchar(16) NOT NULL,
                                Code varchar(16) NOT NULL,
                                Language varchar(16) NOT NULL,
                                Description varchar(1024) NOT NULL,
                                PRIMARY KEY (Category)
                                ); ''')
            print('Created code table')

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='address' ''')
        if c.fetchone()[0] == 0:
            c.execute(''' CREATE TABLE address (
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
            print('Created address table')

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='establishment' ''')
        if c.fetchone()[0] == 0:
            c.execute(''' CREATE TABLE establishment (
                                EstablishmentNumber varchar(16) NOT NULL,
                                StartDate Date NOT NULL,
                                EnterpriseNumber varchar(16) NOT NULL,
                                PRIMARY KEY (EnterpriseNumber),
                                FOREIGN KEY (EnterpriseNumber) REFERENCES enterprise(EnterpriseNumber)
                                ); ''')
            print('Created establishment table')

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='denomination' ''')
        if c.fetchone()[0] == 0:
            c.execute(''' CREATE TABLE denomination (
                                EntityNumber varchar(16) NOT NULL,
                                ActivityGroup varchar(8) NOT NULL,
                                NaceVersion varchar(8) NOT NULL,
                                NaceCode varchar(8) NOT NULL,
                                Classification varchar(4),
                                PRIMARY KEY (EntityNumber),
                                FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                                ); ''')
            print('Created denomination table')

        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='contact' ''')
        if c.fetchone()[0] == 0:
            c.execute(''' CREATE TABLE contact (
                                EntityNumber varchar(16) NOT NULL,
                                EntityContact varchar(8) NOT NULL,
                                ContactType varchar(8) NOT NULL,
                                Value varchar(64) NOT NULL,
                                PRIMARY KEY (EntityNumber),
                                FOREIGN KEY (EntityNumber) REFERENCES enterprise(EnterpriseNumber)
                                ); ''')
            print('Created contact table')

        conn.commit()
        conn.close()

    def populate_tables(self):
        conn = sqlite3.connect('database/example.db')
        c = conn.cursor()

        #skip first line of file
        #read each line, take out data, put it into table

        conn.commit()
        conn.close()

