# https://docs.python.org/3/library/sqlite3.html
from database import Database

db = Database()
db.create_tables()
db.populate_base_tables()
db.populate_company_websites()