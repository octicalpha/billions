import os, shutil
from data.modules.ohlcv_handler import OhlcvHandler
from data.modules.uni_handler import UniHandler

class EssentialDataHandler:
    @staticmethod
    def load_essential_data(essential_data_dir):
        dataHandlerDict = {}
        for edn in os.listdir(essential_data_dir):
            #get correct data handler
            dataname_handlertype = edn.split('.')[0] #some essential data is stored as a file
            dataname_handlertype = dataname_handlertype.split('_')
            dataName = '_'.join(dataname_handlertype[:-1])
            handlerType = dataname_handlertype[-1]
            if(handlerType=='ohlcv'):
                dh=OhlcvHandler('',printProgress=False)
            elif(handlerType=='uni'):
                dh=UniHandler('',printProgress=False)

            #load essential data
            dh.load_essential_data(os.path.join(essential_data_dir, edn))
            dataHandlerDict[dataName] = dh

        return dataHandlerDict

    @staticmethod
    def store_essential_data(essential_data_dir, dataHandlerDict):
        if os.path.isdir(essential_data_dir):
            shutil.rmtree(essential_data_dir)
        os.mkdir(essential_data_dir)
        for dataName, dataHandler in dataHandlerDict.items():
            dataHandler.store_essential_data(essential_data_dir, dataName)