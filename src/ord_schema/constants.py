import dotenv
import os
from pathlib import Path


dotenv.load_dotenv()

DATA_DIR = Path(__file__).parent / 'data'

ENV = dict(
    PG_USERNAME = 'hobs',
    PG_PASSWORD = '',
    PG_HOST = 'localhost',
    PG_PORT = 5432,
    PG_DATABASE = 'ord',
)
ENV.update(dict(os.environ))
PG_URL = (
    'postgresql+psycopg2://'
    + f'{ENV["PG_USERNAME"]}'
    + f':{ENV["PG_PASSWORD"]}'
    + f'@{ENV["PG_HOST"]}'
    + f':{ENV["PG_PORT"]}'
    + f'/{ENV["PG_DATABASE"]}'
    )
ENV['PG_URL'] = PG_URL

for k in ENV:
    locals()[k] = ENV[k]

