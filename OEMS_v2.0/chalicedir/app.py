from ast import literal_eval as le
from chalice import Chalice
from verify2 import verified

app = Chalice(app_name='omstest3')


@app.route('/', methods=['POST'])
def index():
    args = le(app.current_request.raw_body)
    ip = app.current_request.to_dict().get("context", {}).get("identity", {}).get("sourceIp")
    #    return {'ip':ip, 'args':args }
    return {'verified': verified(args, ip)}

#    return {'hello': verified("kenneth", ip)  }

# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
