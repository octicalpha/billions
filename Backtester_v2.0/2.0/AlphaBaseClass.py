"""
Main file that people will use to create models
"""
from data.modules.essential_data_handler import EssentialDataHandler
import itertools

class AlphaBaseClass:
    def load_data_wrapper(self, mode, altsim_dir, download, essential_data_dir):
        assert mode in ['in_lambda_instance', 'launching_grid_search']

        if mode != 'in_lambda_instance':
            dataHandlerDict = self.load_data(altsim_dir, download)
        else:
            dataHandlerDict = EssentialDataHandler.load_essential_data(essential_data_dir)

        if mode == 'launching_grid_search':
            EssentialDataHandler.store_essential_data(essential_data_dir, dataHandlerDict)
        else:
            return dataHandlerDict

    def get_param_combos(self, paramOptions):
        paramLists = [pl for _, pl in paramOptions.items()]
        paramNames = paramOptions.keys()
        paramComboTupes = list(itertools.product(*paramLists))
        for i in range(len(paramComboTupes)):
            paramDict = {}
            for j, paramName in enumerate(paramNames):
                paramDict[paramName] = paramComboTupes[i][j]
            paramComboTupes[i] = paramDict
        return paramComboTupes

    def load_data(self):
        raise NameError('Not implemented!')

    def get_param_dict_for_grid_search(self):
        raise NameError('Not implemented!')

    def get_param_dict_for_single_run(self):
        raise NameError('Not implemented!')

    def generate(self, dataHandlers, params):
        """
        :param alpha: set of weights
        :param data_index: index where to get data from
        :return: normalized weights
        Main Model Code
        """
        raise NameError('Not implemented!')