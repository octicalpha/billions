import os, argparse, ast, importlib
from grid_search_tools.grid_search_signal_sender import GridSearchSignalSender
from grid_search_tools.deploy_lambda_package import deploy_lambda_package


def get_files_to_package(alphaFileName, statsFileName):
    relevant_paths = []
    altsimSrc=os.path.join('data', 'modules')
    lambdaPackageDst='data'
    relevant_paths.append((altsimSrc, lambdaPackageDst))

    altsimSrc = 'util'
    lambdaPackageDst = ''
    relevant_paths.append((altsimSrc, lambdaPackageDst))

    altsimSrc = 'indicator.py'
    lambdaPackageDst = ''
    relevant_paths.append((altsimSrc, lambdaPackageDst))

    altsimSrc = 'AlphaBaseClass.py'
    lambdaPackageDst = ''
    relevant_paths.append((altsimSrc, lambdaPackageDst))

    altsimSrc = os.path.join('grid_search_tools', 'grid_search_lambda_handler.py')
    lambdaPackageDst = ''
    relevant_paths.append((altsimSrc, lambdaPackageDst))

    relevant_paths.append((alphaFileName, lambdaPackageDst))
    relevant_paths.append((statsFileName, lambdaPackageDst))

    return relevant_paths

def run():
    """
    --start : startdate of the simulation
    --end : enddate of the simulation
    --uni : universe name to be simulated
    --book : booksize to be tested, stats will calculate based on the starting book size
    --bookfloat : let booksize float, stats calculate differently if booksize floats, useful for event driven type models
    --local : we may consider loading data locally to avoid overload on db or reduce time consumption
    """

    parser = argparse.ArgumentParser(description='Run backtest on generate.py')
    parser.add_argument('-s', '--start', help='start date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('-e', '--end', help='end date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('-af', '--alphafile', help='file name where AlphaMain is located', type=str, default='generate.py')
    parser.add_argument('-sf', '--statsfile', help='file name where get_stats is located', type=str, default='stats.py')
    parser.add_argument('-b', '--book', help='booksize to be tested', type=int, default='1000000')
    parser.add_argument('--tcost', help='assign tcost', type=float, default=0)
    parser.add_argument('--dload', help='True: download data, False: no download', default='False', type=str)
    parser.add_argument('-up', '--uploadPackage', help='True: Re-upload lambda package., False: Use existing lambda package',
                        default='True', type=str)

    args = parser.parse_args()
    startdate = args.start
    enddate = args.end
    alpha_file_name = args.alphafile
    stats_file_name = args.statsfile
    book = args.book
    tcost = args.tcost
    download = ast.literal_eval(args.dload)
    uploadPackage = ast.literal_eval(args.uploadPackage)

    altsim_dir = os.path.dirname(os.path.realpath(__file__))
    alphaPath = os.path.join(altsim_dir, alpha_file_name)
    deployment_package_dir = os.path.join(altsim_dir, 'deployment_packages', 'altsim_deployment_package')
    essential_data_dir = os.path.join(deployment_package_dir, 'essential_data')

    #init
    spec = importlib.util.spec_from_file_location("", alphaPath)
    alphaModule = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(alphaModule)
    AlphaClass = alphaModule.AlphaMain(startdate, enddate, book)

    # load essential data as specified by alpha file + store in lambda deployment package
    if(uploadPackage):
        AlphaClass.load_data_wrapper('launching_grid_search', altsim_dir, download, essential_data_dir)

        #get extra files/folders to store in lambda deployment package
        src_dst_relative_path_tupes = get_files_to_package(alpha_file_name, stats_file_name)

        #deploy lambda package
        deploy_lambda_package(deployment_package_dir, altsim_dir, 'Archive.zip', src_dst_relative_path_tupes)

    #generate params
    paramDict = AlphaClass.get_param_dict_for_grid_search()
    paramCombos = AlphaClass.get_param_combos(paramDict)

    #invoke backtests in parallel
    gridSearchSignalSender = GridSearchSignalSender(altsim_dir, alpha_file_name)
    alphaMainArgs = {'startdate':startdate,'enddate':enddate,'booksize':book}
    statsArgs = {'tcost': tcost}
    gridSearchSignalSender.run_grid_search(paramCombos, alphaMainArgs, statsArgs, alpha_file_name, stats_file_name)

if __name__ == '__main__':
    run()

