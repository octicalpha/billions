import os
import datetime
import zipfile
from flask import send_file, Flask
from io import BytesIO
app = Flask(__name__)


def filter_by_key(key, files):
    return list(sorted(filter(lambda f: key in f, files)))


def create_zip_file(base_dir, files, zip_name, in_memory=False):
    file_name = zip_name
    if in_memory:
        zip_name = BytesIO()
    with zipfile.ZipFile(zip_name, 'w') as myzip:
        for f in files:
            myzip.write(base_dir + "/" + f)
    if in_memory:
        zip_name.seek(0)
    return zip_name, file_name


def create_multiple_zip_file(paths):
    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w') as myzip:
        for path in paths:
            myzip.write(path)
    memory_file.seek(0)
    return memory_file


def get_latest_files(dir_path, file_keys, in_memory=False):
    files = os.listdir(dir_path)
    file_names = map(lambda key: filter_by_key(key, files)[-1], file_keys)
    dt = datetime.datetime.fromtimestamp(int(file_names[0].split(".")[-2])).strftime('%Y%m%d.%I_%M_%S%p')
    return create_zip_file(dir_path, file_names, '{}.{}.zip'.format(dir_path, dt), in_memory=in_memory)


def get_latest_bitflyer(in_memory=False):
    return get_latest_files('bitflyer_data', ['deposits', 'withdrawals', 'addresses', 'balance'], in_memory=in_memory)


def get_latest_kraken(in_memory=False):
    return get_latest_files('kraken_data', ['deposits', 'withdrawals', 'balance', 'ledger'], in_memory=in_memory)


def get_latest_bittrex(in_memory=False):
    return get_latest_files('bittrex_data', ['deposits', 'withdrawals', 'addresses', 'balance', 'trade'],
                            in_memory=in_memory)


def get_latest_gdax(in_memory=False):
    return get_latest_files('gdax_data', ['balance'],
                            in_memory=in_memory)


def get_dates():
    pass
    # TODO: get a list of all possible dates


def get_latest(f):
    memory_file, file_name = f(in_memory=True)
    return send_file(memory_file, attachment_filename=file_name, as_attachment=True)


@app.route("/files/kraken")
def kraken_files():
    return get_latest(get_latest_kraken)


@app.route("/files/bitflyer")
def bitflyer_files():
    return get_latest(get_latest_bitflyer)


@app.route("/files/bittrex")
def bitflyer_files():
    return get_latest(get_latest_bittrex)


@app.route("/files/gdax")
def gdax_files():
    return get_latest(get_latest_gdax)


@app.route("/")
def hello():
    return app.send_static_file('main.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
