from flask import Flask
from flask import request, jsonify
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

import json


app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024


@app.route('/<tenant_id>/create_table_if_not_exist', methods=['POST'])
def create_table_if_not_exist(tenant_id):
  # check request method
  if request.method != 'POST':
    return jsonify({'msg': 'Incorrect request method'}), 400

  # get requset payload
  table = request.json.get('table', None)
  if not table:
    return jsonify({'msg': 'Missing parameter'}), 400

  # prepare cql sentence
  columns = ','.join(['{} {}'.format(item['field'], item['type']) for item in table['schema']])+ ', PRIMARY KEY ({})'.format(','.join(item for item in table['primary_key']))
  cluster = Cluster(['35.228.109.23'])
  session = cluster.connect()
  create_stmt = session.prepare("CREATE TABLE IF NOT EXISTS {}.{} ({}) ;".format(tenant_id, table['table_name'], columns))
  session.execute(create_stmt)

  return jsonify({'msg': 'success'}), 200


@app.route('/<tenant_id>/batch_ingest', methods=['POST'])
def batch_ingest(tenant_id):
  # check request method
  if request.method != 'POST':
    return jsonify({'msg': 'Incorrect request method'}), 400
  if not request.is_json:
    return jsonify({"msg": "Missing JSON in request"}), 400

  # get request payload
  table_name = request.json.get('table_name', None)
  rows = request.json.get('rows', None)

  # connect cassandra
  cluster = Cluster(['35.228.109.23'])
  session = cluster.connect()
  batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

  count = 0

  for row in rows:
    keys = row.keys()
    fields = ",".join([item for item in keys])
    # cql = "INSERT INTO {}.{} ({}) VALUES ({})".format(tenant_id, table_name, fields, values)
    insert_stmt ="INSERT INTO {}.{} ({}) VALUES ({})".format(tenant_id, table_name, fields, ",".join(["%s" for item in keys]))
    # session.execute(insert_stmt, [row[item] for item in keys])
    batch.add(insert_stmt, [row[item] for item in keys])
    count+=1
  # insert every 50 rows
  session.execute(batch)
  batch.clear()
  return jsonify({'msg': 'success', "rows": count}), 200


@app.route('/<tenant_id>/stream_ingest', methods=['POST'])
def streaming_ingest(tenant_id):
    # check request method
  if request.method != 'POST':
    return jsonify({'msg': 'Incorrect request method'}), 400
  if not request.is_json:
    return jsonify({"msg": "Missing JSON in request"}), 400

  # get request payload
  table_name = request.json.get('table_name', None)
  data = request.json.get('data', None)

  # connect cassandra
  cluster = Cluster(['35.228.109.23'])
  session = cluster.connect()
  batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

  # insert data to cassandra
  keys = data.keys()
  fields = ",".join([item for item in keys])
  insert_stmt ="INSERT INTO {}.{} ({}) VALUES ({})".format(tenant_id, table_name, fields, ",".join(["%s" for item in keys]))
  session.execute(insert_stmt, [data[item] for item in keys])
  return  jsonify({'msg': 'success'}), 200


if __name__ == '__main__':
  app.run(port=5000)
