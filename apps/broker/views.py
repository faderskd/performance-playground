from flask import Blueprint, request, jsonify
from pydantic import ValidationError

from apps.broker.db import BrokerDb, DbRecord
from apps.broker.models import Record

app = Blueprint('broker', __name__, template_folder='templates', url_prefix='/broker')
db = BrokerDb()


@app.route('/append/', methods=['POST'])
def append():
    try:
        record = Record.model_validate_json(request.data)
        offset = db.append_record(DbRecord.from_model(record))
        return jsonify({'offset': offset})
    except ValidationError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/read/<offset>', methods=['GET'])
def read(offset: int):
    try:
        offset = int(offset)
        record = db.read_record(offset).to_model()
        return jsonify(record.model_dump())
    except ValidationError as e:
        return jsonify({'error': str(e)}), 400
