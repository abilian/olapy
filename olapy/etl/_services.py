import dotenv
import logging
from bonobo_sqlalchemy.util import create_postgresql_engine

dotenv.load_dotenv(dotenv.find_dotenv())
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

DB_NAME = 'tutorial'
USER = 'postgres'
PASS = 'root'


def get_services():
    return {
        'sqlalchemy.engine':
            create_postgresql_engine(
                **{'name': DB_NAME,
                   'user': USER,
                   'pass': PASS})
    }
