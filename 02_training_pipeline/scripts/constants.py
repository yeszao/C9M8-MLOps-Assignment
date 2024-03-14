from pathlib import Path

ROOT_PATH = Path(__file__).parent.parent.parent

DB_PATH = ROOT_PATH.joinpath('01_data_pipeline/scripts')
DB_FILE_NAME = 'utils_output.db'

DB_FILE_MLFLOW = ''

TRACKING_URI = ''
EXPERIMENT =''


# model config imported from pycaret experimentation
model_config = ''

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ''
# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']
