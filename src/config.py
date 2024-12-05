import os
from dotenv import load_dotenv

load_dotenv(override=True)

rate_limits = dict(
    per_second=20 - 3,  # 3 requests as buffer so RITO doesn't ban us O:)
    per_minute=50 - 3,
)

riot_api = dict(
    puuid=os.getenv('LOL_PUUID'),
    match_snapshot=os.getenv('LOL_MATCH_SNAPSHOT'),
)

secrets = dict(
    api_key=os.getenv('API_KEY'),
)

postgres = dict(
    connection_string=os.getenv('DB_CONNECTION_STRING'),
    user=os.getenv('DB_USER'),
    secret=os.getenv('DB_SECRET'),
    db_name=os.getenv('DB_NAME'),
    port=os.getenv('DB_PORT'),
    host=os.getenv('DB_HOST')
)

endpoints = dict(
    lol_base_url=os.getenv('LOL_BASE_URL')
)

logging = dict(
    log_file=os.getenv('LOG_FILE'),
)

exports = dict(
    csv_export_dir=os.getenv('CSV_EXPORT_DIR'),
    match_files_dir=os.getenv('LOL_MATCH_FILES_DIR'),
)

# TODO: make more generic
PUUIDS = [
    'FiB--8fS9Kzsy8zwtz0afbpDSFc1GPtSvnH9jqBkwWGABj1ZN2bRMU2rXar6M31jXBKLlo_sfVUT_w',
    'YkhJDHjPwoRT6sBHoP8bDmIbWL5E5wZ5fOBjYZm9ACA2J5BkI_cbzZmBJMIH2ruLJxQwSLh1slBi7w'
]
