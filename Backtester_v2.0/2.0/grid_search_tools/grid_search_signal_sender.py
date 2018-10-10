import time, os, json, traceback
import boto3
from dotenv import load_dotenv, find_dotenv
from util.payload_db_handler import PayloadDBHandler
from grid_search_tools.backtest_status import BackTestStatusBatch
load_dotenv(find_dotenv())

class GridSearchSignalSender:
    def __init__(self, altsim_dir, alpha_file_name):
        resultsName = '{}_grid_search_results_{}'.format(alpha_file_name.split('.')[0], int(time.time()))
        self.ALPHA_FILE_NAME = alpha_file_name

        self.RESULTS_FILE_PATH = os.path.join(altsim_dir, 'results', 'grid_search', '{}.csv'.format(resultsName))
        self.BACKTEST_STATUS_PATH = os.path.join(altsim_dir, 'results', 'back_test_status', '{}.csv'.format(resultsName))

        self.RESUME_GRID_SEARCH = False #TODO implement this

        self.MAX_PARALELISM = 1000
        self.MAX_WAIT_TIME = 60 * 5 + 30

        self.AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
        self.AWS_ACCESS_SECRET = os.environ.get("AWS_ACCESS_SECRET")
        self.REGION_NAME = os.environ.get("REGION_NAME")

        self.TABLE_NAME = resultsName
        self.FUNCTION_NAME = os.environ.get("FUNCTION_NAME")
        self.BUCKET_NAME = os.environ.get("BUCKET_NAME")
        self.PAYLOAD_ID = 'alphaGridSearch'

        self.lambda_client = boto3.client(
            'lambda',
            aws_access_key_id=self.AWS_ACCESS_KEY,
            aws_secret_access_key=self.AWS_ACCESS_SECRET,
            region_name=self.REGION_NAME)
        self.dbHandler = PayloadDBHandler(self.AWS_ACCESS_KEY,
                                     self.AWS_ACCESS_SECRET,
                                     self.REGION_NAME,
                                     self.TABLE_NAME)
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.AWS_ACCESS_KEY,
            aws_secret_access_key=self.AWS_ACCESS_SECRET,
            region_name=self.REGION_NAME)

    def initialize_grid_search(self):
        print('Initializing Dynamo DB where grid search results will be temporarily stored.')
        self.dbHandler.init_table()

    def clean_up_grid_search(self):
        print('Delteing Dynamo DB grid search results table.')
        self.dbHandler.delete_table()

    def run_grid_search(self, paramCombos, alphaMainArgs, statsArgs, alphaFileName, statsFileName):
        self.initialize_grid_search()

        print('Running grid search on {} param combos.'.format(len(paramCombos)))

        try:
            btsb = BackTestStatusBatch(self.BACKTEST_STATUS_PATH, self.RESUME_GRID_SEARCH, paramCombos)
            getResults = False
            for i, bt in enumerate(btsb.statuses):
                if (bt.waiting_to_launch()):
                    payload = {'alphaParams': bt.params,
                               'alphaMainArgs': alphaMainArgs,
                               'statsArgs': statsArgs,
                               'ALPHA_FILE_NAME': alphaFileName,
                               'STATS_FILE_NAME': statsFileName,
                               'AWS_ACCESS_KEY': self.AWS_ACCESS_KEY,
                               'AWS_ACCESS_SECRET': self.AWS_ACCESS_SECRET,
                               'REGION_NAME': self.REGION_NAME,
                               'TABLE_NAME': self.TABLE_NAME,
                               'PAYLOAD_ID': self.PAYLOAD_ID,
                               'SEQUENCE': bt.sequence
                               }

                    # launch lambda instance
                    self.lambda_client.invoke(
                        FunctionName=self.FUNCTION_NAME,
                        InvocationType='Event',
                        Payload=json.dumps(payload),
                    )

                    btsb.update_bt_status(bt, 'inProgress')

                    if not btsb.btInProgress + btsb.btComplete % 100:
                        print('Back Tests Launched: {}, Back Tests To Launch{}'.format(btsb.btInProgress + btsb.btComplete,
                                                                                       btsb.btToLaunch))

                # check if results should be fetched
                if (btsb.btInProgress == self.MAX_PARALELISM):
                    print('Reached max paralelism. Cur:{}, Max:{}.'.format(btsb.btInProgress, self.MAX_PARALELISM))
                    getResults = True
                # check if all sequences have been launched
                if (btsb.btToLaunch == 0):
                    print('All backtests have been launched. Waiting for completion.')
                    getResults = True

                if (getResults):
                    getResults = False

                    # WAIT FOR A BATCH OF BACKTESTS TO COMPLETE
                    while btsb.btInProgress > 0:
                        curTime = time.time()

                        # get batch of results
                        resultBatch = []
                        for bt in btsb.statuses:
                            if (bt.inProgress):
                                result = self.dbHandler.get_payload(self.PAYLOAD_ID, bt.sequence)
                                if (result is not None):
                                    btsb.update_bt_status(bt, 'complete')
                                    resultBatch.append(result)
                                elif (curTime - bt.startTime > self.MAX_WAIT_TIME):
                                    btsb.update_bt_status(bt, 'failed')

                                    result = bt.params
                                    result['success'] = False
                                    result['error'] = 'Could not find results in db.'
                                    resultBatch.append(result)

                        if (len(resultBatch) > 0):
                            # STORE RESULTS
                            with open(self.RESULTS_FILE_PATH, 'a') as f:
                                for result in resultBatch:
                                    f.write('{}\n'.format(json.dumps(result)))
                            print('Stored {} results in {}.'.format(len(resultBatch), self.RESULTS_FILE_PATH))

                            # STORE BACKTEST STATUSES
                            btsb.store_statuses()

                            # these lambda instances have completed
                            print('Backtests Complete: {}, Backtests In Progress: {}'.format(btsb.btComplete, btsb.btInProgress))
                            # break out of loop and launch more backtests if there are still more left to launch
                            if (btsb.btToLaunch != 0):
                                break
                            elif (btsb.btInProgress == 0):
                                break
                        else:
                            print('Waiting for results...')
                            time.sleep(5)
        except:
            print('Grid Search Error\n{}'.format(traceback.format_exc()))

        self.clean_up_grid_search()

        print('Finished Grid Search.')