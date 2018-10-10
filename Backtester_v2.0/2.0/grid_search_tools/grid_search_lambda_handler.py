import os, traceback, importlib
from payload_db_handler import PayloadDBHandler

def lambda_handler(event, context):
    try:
        #init
        spec = importlib.util.spec_from_file_location("", event['ALPHA_FILE_NAME'])
        alphaModule = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(alphaModule)
        AlphaClass = alphaModule.AlphaMain(event['alphaMainArgs']['startdate'],
                                           event['alphaMainArgs']['enddate'],
                                           event['alphaMainArgs']['booksize'])

        #load essential data
        essential_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'essential_data')
        dataHandlerDict = AlphaClass.load_data_wrapper(mode='in_lambda_instance', altsim_dir=None, download=None,
                                     essential_data_dir=essential_data_dir)

        #run backtest
        alpha = AlphaClass.generate(dataHandlerDict, event['alphaParams'])

        spec = importlib.util.spec_from_file_location("", event['STATS_FILE_NAME'])
        statsModule = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(statsModule)

        results = statsModule.get_stats(alpha, AlphaClass.ohlcv_close, AlphaClass.booksize,
                                           0, tcost=event['statsArgs']['tcost'])

        payload = {
            'alphaParams':event['alphaParams'],
            'results': results,
            'success': True
        }
    except Exception as e:
        payload = {
            'alphaParams': event['alphaParams'],
            'error': traceback.format_exc(),
            'success': False
        }

    dbHandler = PayloadDBHandler(event['AWS_ACCESS_KEY'],
                                 event['AWS_ACCESS_SECRET'],
                                 event['REGION_NAME'],
                                 event['TABLE_NAME'])
    dbHandler.init_table()
    dbHandler.set_payload(event['PAYLOAD_ID'], event['SEQUENCE'], payload)