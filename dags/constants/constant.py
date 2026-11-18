from airflow.models import Variable

S3_BUCKET = Variable.get("s3_bucket")
TRIGGER_FILE_NAME = Variable.get("trigger_file_name")

TRIGGER_CHECKER_PATH_COORDINATES = Variable.get("trigger_checker_path_coordinates")
INPUT_TRIGGER_CHECKER_PATH = Variable.get("input_trigger_checker_path")
MASKED_INPUT_TRIGGER_CHECKER_PATH = Variable.get("masked_input_trigger_checker_path")
PROCESSED_INPUT_TRIGGER_CHECKER_PATH = Variable.get("processed_input_trigger_checker_path")

INPUT_DATA_PATH = Variable.get("input_data_path")
MASKED_DATA_PATH = Variable.get("masked_data_path")
PROCESSED_DATA_PATH = Variable.get("processed_data_path")
