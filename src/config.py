import os
from dotenv import load_dotenv

load_dotenv(override=True)

secrets = dict(
	puuid = os.getenv('LOL_PUUID'),
	api_key = os.getenv('RIOT_TOKEN'),
)

postgres = dict(
    connection_string = os.getenv('DB_CONNECTION_STRING'),
	user = os.getenv('DB_USER'),
	secret = os.getenv('DB_SECRET'),
	db_name = os.getenv('DB_NAME'),
 	port = os.getenv('DB_PORT'),
	host = os.getenv('DB_HOST')
)

endpoints = dict(
	lol_base_url = os.getenv('LOL_BASE_URL')
)

