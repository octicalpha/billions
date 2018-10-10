from flask import Flask, request, jsonify
from flask.json import JSONEncoder
from flask_restful import reqparse, abort, Api, Resource
from verify import verified
from collections import defaultdict
import json
import traceback

import os
from datetime import datetime
import ast
import pandas as pd

from analytics.oms_data_analytics import OMSDataAnalytics

class JSONEncoderTimestamp(JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        elif (type(obj) == pd.Timestamp or type(obj) == datetime):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

app = Flask(__name__)
app.json_encoder = JSONEncoderTimestamp
api = Api(app)

parser = reqparse.RequestParser()
args=['botName']
for arg in args:
    parser.add_argument(arg)

class BotInfo(Resource):
    def post(self):
        args = parser.parse_args()

        if not verified(args):
            abort(404, message='Something went wrong in verifying forbidden')

        try:
            OMSDataAnalyzer=OMSDataAnalytics(args['botName'])
            return OMSDataAnalyzer.get_current_pnl()
        except:
            abort(404, message='{} stat analysis failed'.format(args['botName']))

api.add_resource(BotInfo, '/botinfo')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8889)