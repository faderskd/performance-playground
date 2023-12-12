import os
import random
import string


def random_string(length=10):
    choices = string.ascii_uppercase + string.digits
    return ''.join([random.choice(choices) for _ in range(length)])


def ensure_file_not_exists_in_current_dir(file_name) -> str:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    test_db_file_path = os.path.join(dir_path, file_name)
    if os.path.exists(test_db_file_path):
        os.remove(test_db_file_path)
    return test_db_file_path
