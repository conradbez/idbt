import os
import glob 
from pathlib import Path

DBT_PROJECT_NAME = 'dbt_project'
IDBT_DIR = os.path.dirname(os.path.abspath(__file__))
DUCK_DB_PATH = str(IDBT_DIR + "/idbt.duckdb")

DBT_CLI_ARGS =  [
    "--vars",
    "{IDBT_DUCKDB_PATH: IDBT_DIR}".replace('IDBT_DIR', str(DUCK_DB_PATH)), # set duckdb path
    "--project-dir", 
    f"{IDBT_DIR}/{DBT_PROJECT_NAME}", 
    "--profiles-dir", 
    IDBT_DIR]
IDBT_MODEL_DIR = os.path.join(IDBT_DIR, DBT_PROJECT_NAME, 'models', 'idbt',)
os.environ['DBT_PROFILES_DIR'] = str(IDBT_DIR) + '/idbt.duckdb'

TEMPLATE_NAMES = [os.path.splitext(os.path.basename(f))[0] for f in glob.glob(str(IDBT_DIR) + "/templates/*", )]
DATA_SOURCE_NAMES = [os.path.basename(f) for f in glob.glob(str(Path(IDBT_DIR) / 'dbt_project' / 'seeds') + "/*", )]