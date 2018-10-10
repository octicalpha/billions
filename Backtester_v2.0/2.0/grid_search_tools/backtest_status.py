import pickle, time

class BackTestStatus:
    def __init__(self, dict):
        self.sequence = dict['sequence']
        self.inProgress = dict['inProgress']
        self.complete = dict['complete']
        self.failed = dict['failed']
        self.startTime = dict['startTime']
        self.params = dict['params']
        self.paramHash = hash(frozenset(self.params))
        self.stored = False

    def set_status(self, status):
        assert status in ['inProgress', 'complete', 'failed']
        if (self.complete or self.failed):
            raise NameError('Can\'t set status after completion or failure.')
        self.inProgress = False
        setattr(self, status, True)

    def can_store(self):
        return not self.stored and (self.failed or self.complete)

    def waiting_to_launch(self):
        return not self.complete and not self.inProgress and not self.failed

class BackTestStatusBatch:
    def __init__(self, backtestStatusesPath, resumeBacktests, paramCombos):
        self.backtestStatusesPath = backtestStatusesPath
        self.statuses = self.load_statuses(resumeBacktests, paramCombos)

        self.btInProgress = len([bt for bt in self.statuses if bt.inProgress])
        self.btComplete = len([bt for bt in self.statuses if bt.complete])
        self.btToLaunch = len([bt for bt in self.statuses if bt.waiting_to_launch()])

    def store_statuses(self):
        with open(self.backtestStatusesPath, 'wb') as f:
            for bt in self.statuses:
                if bt.can_store():
                    bt.stored = True
                    pickle.dump(bt.__dict__, f)

    def load_statuses(self, resumeBacktests, paramCombos):
        if(resumeBacktests):
            backTestStatusDicts = []
            with open(self.backtestStatusesPath, 'rb') as f:
                while 1:
                    try:
                        backTestStatusDicts.append(pickle.load(f))
                    except EOFError:
                        break
            backTestStatuses = [BackTestStatus(d) for d in backTestStatusDicts]

            #TODO check which params have been backtested... re-add ones which haven't
            raise NameError('Backtest resumption not implemented.')
        else:
            backTestStatuses = [
                BackTestStatus({'sequence': i, 'inProgress': False, 'complete': False, 'startTime': None, 'failed': False,
                                'params':pc})
                for i,pc in enumerate(paramCombos)]

        return backTestStatuses

    def update_bt_status(self, bt, status):
        bt.set_status(status)

        if(status == 'inProgress'):
            bt.startTime = time.time()
            self.btInProgress += 1
            self.btToLaunch -= 1
        elif(status == 'complete'):
            self.btInProgress -= 1
            self.btComplete += 1
        elif(status == 'failed'):
            self.btInProgress -= 1