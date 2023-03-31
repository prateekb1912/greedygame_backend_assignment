import json
from flask import Flask, jsonify, request

from datastore import Datastore

app = Flask(__name__)

store = Datastore()

@app.route('/', methods=['POST'])
def index():
    try:
        input = json.loads(request.get_data())
        cmd = input.get('command')

        if not cmd:
            return {'error': 'dsssd'}

        parts = cmd.split()

        cmd_type = parts[0]
        key = parts[1]

        if cmd_type == 'SET':
            value = parts[2]
            expiry_time = None
            condition = None

            if len(parts) > 3:
                if parts[3] == 'EX':
                    expiry_time = int(parts[4])
                else:
                    condition = parts[-1]
            
            resp = store.set(key, value, expiry_time, condition)

            code = resp[0]
            message = resp[1]

            if code == 400:
                return jsonify({'error': message}), 400

            return jsonify({'success': message}), 200
        
        if cmd_type == 'GET':
            val = store.get(key)

            if val is None:
                return jsonify({'error': 'key not found'}), 404
            
            return jsonify({'value': val}), 200
        
        if cmd_type == 'QPUSH':
            vals = parts[2:]

            store.qpush(key, vals)

            return jsonify({'success': 'Values added to queue'}), 200

        if cmd_type == 'QPOP':
            resp, code = store.qpop(key)

            return jsonify(resp),code

        if cmd_type == 'BQPOP':
            timeout = int(parts[2])

            resp, code = store.bqpop(key, timeout)

            return jsonify(resp), code

        else:
            return jsonify({'error': 'Invalid command.'}), 400
 
    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run()
