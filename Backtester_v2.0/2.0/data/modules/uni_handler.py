import pandas as pd
import os, shutil

class UniHandler:
    def __init__(self,  altsimDir, env='aws_dev', printProgress=True):
        self.querySkeleton = """select * from universe.{} where time >= '{}' and time <= '{}' order by time asc"""
        self.fileNameSkeleton = 'universe-{}.csv'
        self.dataDir = os.path.join(altsimDir, 'data', 'universe')

        self.env = env
        self.printProgress = printProgress

        self.MAX_NA_PCT = .05

        self.essentialDataStorageFileSkeleton = '{}_uni.csv'

        self.essentialData = None

    def pp(self, msg):
        if (self.printProgress):
            print(msg)

    def check_max_na(self, name, data):
        pctNan = data.isna().sum().max() / float(len(data))
        if (pctNan > self.MAX_NA_PCT):
            raise NameError('{}pct of {} is nan!'.format(pctNan, name))

    def filter_uni(self,uni_df,exch,base,quote,exclude,short_case):
        """
        def filter_uni = filter out exch and quote curr
        """

        uni_df = uni_df.loc[:,uni_df.sum()>0]
        chosenCols = []
        for col in uni_df:
            b,q,e=col.split('-')
            if(e in exch and q in quote and
                (len(base)==0 or b in base) and
                (len(exclude)==0 or b not in exclude)):

                if len(short_case) > 0:
                    short_exch = [i.split("-")[2] for i in short_case]
                    if e in short_exch and col in short_case:
                        chosenCols.append(col)
                else:
                    chosenCols.append(col)
        return uni_df[chosenCols]

    def get_data(self, uniName, sd, ed, download,
                 exch, base, quote, exclude, short_case):
        localPath = os.path.join(self.dataDir, self.fileNameSkeleton.format(uniName))

        if(download):
            self.pp('Fetching {} from DB.'.format(uniName))
            from util.postgresConnection import query  # avoid having to add postgres tools to lambda package
            data = query(self.querySkeleton.format(uniName, sd, ed), environment=self.env, dataframe=True)

            #duplicates stored in db
            data.drop_duplicates('time', inplace=True)
            #time stored as text in db
            data['time'] = pd.to_datetime(data['time'])
            data.set_index('time', inplace=True)
            #uni membership stored as 'true', 'false' instead of bool in db
            data = data == 'true'

            if os.path.isfile(localPath):
                self.pp('Merging {} with local data.'.format(uniName))
                localData = pd.read_csv(localPath, index_col=0)
                localData.set_index(pd.to_datetime(localData.index), inplace=True)

                mask = (localData.index < sd) | (localData.index > ed)
                localData = localData[mask]

                data = pd.concat([data, localData], axis=0)
                data.sort_index(inplace=True)

            data.to_csv(localPath)
        else:
            self.pp('Fetching {} locally.'.format(uniName))
            data = pd.read_csv(localPath, index_col=0)
            data.set_index(pd.to_datetime(data.index), inplace=True)

        data = data.loc[sd:ed, :]
        self.check_max_na(uniName, data)

        self.essentialData = self.filter_uni(data, exch, base, quote, exclude, short_case)

    def store_essential_data(self, essential_data_dir, dataName):
        if (self.essentialData is None):
            raise NameError('Must call "get_data" prior to calling "store_essential_data".')

        essential_universe_file = os.path.join(essential_data_dir, self.essentialDataStorageFileSkeleton.format(dataName))

        if(os.path.isfile(essential_universe_file)):
            shutil.rmtree(essential_universe_file)
        self.essentialData.to_csv(essential_universe_file)

    def load_essential_data(self, essential_universe_file):
        if(not os.path.isfile(essential_universe_file)):
            raise NameError('Essential data was never stored.')

        uni = pd.read_csv(essential_universe_file, index_col=0)
        uni.set_index(pd.to_datetime(uni.index), inplace=True)

        self.essentialData = uni
