import random
import string


def random_string(length=10):
    choices = string.ascii_uppercase + string.digits
    return ''.join([random.choice(choices) for _ in range(length)])
