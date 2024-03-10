# You can create more variables according to your project. The following are the basic variables that have been provided to you
from pathlib import Path

SCRIPTS_DIRECTORY = Path(__file__).parent

DB_PATH = SCRIPTS_DIRECTORY
DB_FILE_NAME = 'utils_output.db'
UNIT_TEST_DB_FILE_NAME = ''
DATA_DIRECTORY = SCRIPTS_DIRECTORY.joinpath('data')
INTERACTION_MAPPING = ''
INDEX_COLUMNS_TRAINING = []
INDEX_COLUMNS_INFERENCE = []
NOT_FEATURES = []




