from flask import Blueprint, request, jsonify
from pydantic import ValidationError

from apps.broker.storage.storage_engine import DbRecord
from apps.broker.models import Record

FILE_NAME = 'db' # TODO move to config

app = Blueprint('broker', __name__, template_folder='templates', url_prefix='/broker')
# db = DbEngine(FILE_NAME)


@app.route('/append/', methods=['POST'])
def append():
    try:
        record = Record.model_validate_json(request.data)
        offset = db.append_record(DbRecord.from_model(record))
        return jsonify({'offset': offset})
    except ValidationError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/read/<offset>/', methods=['GET'])
def read(offset: int):
    offset = int(offset)
    record = db.read_record(offset).to_model()
    return jsonify(record.model_dump())
