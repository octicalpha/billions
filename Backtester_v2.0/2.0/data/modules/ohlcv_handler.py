import pandas as pd
import os, shutil

class OhlcvHandler:
    def __init__(self,  altsimDir, env='aws_exchanges', printProgress=True):
        self.querySkeleton = """select * from "{}"."{}" where time >= '{}' and time <= '{}' order by time asc"""
        self.tableNameSkeleton = 'OHLCV_SPOT_{}_{}'
        self.fileNameSkeleton = '{}-OHLCV_SPOT_{}_{}.csv'
        self.dfIndex = '{}-OHLCV_SPOT_{}_{}'
        self.dataDir = os.path.join(altsimDir,'data','ohlcv')

        self.essentialDataStorageDirSkeleton = '{}_ohlcv'

        self.env = env
        self.printProgress = printProgress

        self.MAX_NA_PCT = .05

        self.essentialData = None

    def pp(self, msg):
        if(self.printProgress):
            print(msg)

    def tick_2_data_name(self, tick, dataNameType):
        b,q,e=tick.split('-')
        if(dataNameType=='db'):
            return e, self.tableNameSkeleton.format(b,q)
        elif(dataNameType=='file'):
            return e, self.fileNameSkeleton.format(e,b,q)
        elif(dataNameType=='df'):
            return self.dfIndex.format(e,b,q)
        else:
            raise NameError('Invalid dataNameType: {}'.format(dataNameType))

    def check_max_na(self, name, data):
        pctNan=data.isna().sum().max()/float(len(data))
        if(pctNan > self.MAX_NA_PCT):
            raise NameError('{}pct of {} is nan!'.format(pctNan, name))

    #TODO add filtered cols
    def get_data(self, tickers, sd, ed, interval, download):
        dataDict={}
        for tick in tickers:
            e,fn = self.tick_2_data_name(tick, 'file')
            localPath = os.path.join(self.dataDir, e, fn)
            if(download):
                self.pp('Fetching {} from DB.'.format(tick))
                s,tn=self.tick_2_data_name(tick, 'db')
                from util.postgresConnection import query #avoid having to add postgres tools to lambda package
                data = query(self.querySkeleton.format(s,tn,sd,ed), environment=self.env, dataframe=True)

                data.drop(['in_z'], axis=1, inplace=True)
                data.set_index('time', inplace=True)

                if(os.path.isfile(localPath)):
                    self.pp('Merging {} with local data.'.format(tick))
                    localData = pd.read_csv(localPath, index_col=0)
                    localData.set_index(pd.to_datetime(localData.index), inplace=True)

                    mask = (localData.index<sd) | (localData.index>ed)
                    localData = localData[mask]

                    data = pd.concat([data, localData], axis=0)
                    data.sort_index(inplace=True)

                data.to_csv(localPath)
            else:
                self.pp('Fetching {} locally.'.format(tick))
                data = pd.read_csv(localPath, index_col=0)
                data.set_index(pd.to_datetime(data.index), inplace=True)

            data = data.loc[sd:ed,:]
            self.check_max_na(self.tick_2_data_name(tick, 'df'), data)

            data['baseVolume'].fillna(0, inplace=True)
            data['tradeCount'].fillna(0, inplace=True)
            
            data = data.resample(interval).agg({'open': 'first',
                                        'high': 'max',
                                        'low': 'min',
                                        'close': 'last',
                                        'baseVolume': 'sum',
                                        'tradeCount': 'sum'})

            data.fillna(method='ffill', inplace=True)
            data.fillna(method='bfill', inplace=True)
            
            dataDict[self.tick_2_data_name(tick, 'df')] = data

        self.essentialData = dataDict

    def get_col(self, col):
        if(self.essentialData is None):
            raise NameError('Must call "get_data" prior to calling "get_col".')

        def df2col(name):
            e,rest=name.split('-')
            _,_,b,q=rest.split('_')
            return '{}-{}-{}'.format(b,q,e)

        return pd.concat({df2col(ohlcvName): df[col] for ohlcvName,df in self.essentialData.items()}, axis=1)

    def store_essential_data(self, essential_data_dir, dataName):
        if (self.essentialData is None):
            raise NameError('Must call "get_data" prior to calling "store_essential_data".')

        essential_ohlcv_dir = os.path.join(essential_data_dir, self.essentialDataStorageDirSkeleton.format(dataName))
        if(os.path.isdir(essential_ohlcv_dir)):
            shutil.rmtree(essential_ohlcv_dir)
        os.mkdir(essential_ohlcv_dir)
        for ohlcvName, ohlcvData in self.essentialData.items():
            ohlcvData.to_csv(os.path.join(essential_ohlcv_dir, '{}.csv'.format(ohlcvName)))

    def load_essential_data(self, essential_ohlcv_dir):
        if(not os.path.isdir(essential_ohlcv_dir)):
            raise NameError('Essential data was never stored.')

        self.essentialData = {}
        for ohlcvFileName in os.listdir(essential_ohlcv_dir):
            if('.csv' in ohlcvFileName):
                ohlcvName,_ = ohlcvFileName.split('.')
                ohlcvData = pd.read_csv(os.path.join(essential_ohlcv_dir, ohlcvFileName), index_col=0)
                ohlcvData.set_index(pd.to_datetime(ohlcvData.index), inplace=True)
                self.essentialData[ohlcvName] = ohlcvData