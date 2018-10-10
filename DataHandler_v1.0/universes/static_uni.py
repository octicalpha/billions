import os,sys
import pandas as pd
import argparse
from load_instruments import LoadInstruments
from utils.postgresConnection import query

# Country
country_settings = pd.DataFrame( { 'country' : ['USA','USA','USA','USA','KR','KR','KR','USA'] },
                                index = ['BTRX','GDAX','GMNI','KRKN','BTHM','CONE','KBIT','BINA'] )

def get_country_uni(inst,country_input):
    country = pd.DataFrame(country_input)
    inst['country'] = ''
    for index, row in inst.iterrows():
        inst['country'][index] = country['country'][inst['exchange'][index]]
    return inst

def get_base_currency(inst):
    inst['base'] = ''
    inst['quote'] = ''
    for index, row in inst.iterrows():
        ticker = inst['ticker'][index].split("-")
        inst['base'][index] = ticker[0]
        inst['quote'][index] = ticker[1]
    return inst

def create_int(inst):
    inst.exchange = pd.Categorical(inst.exchange)
    inst['exchange_int'] = inst.exchange.cat.codes
    inst.country = pd.Categorical(inst.country)
    inst['country_int'] = inst.country.cat.codes
    inst.base = pd.Categorical(inst.base)
    inst['base_int'] = inst.base.cat.codes
    inst.quote = pd.Categorical(inst.quote)
    inst['quote_int'] = inst.quote.cat.codes
    return inst

if __name__ == '__main__':
    """
    TODO: discuss fixed set for top_uni, bot_uni to make subset universes. ATM creating for all is to extensive and resource extensive
    """
    parser = argparse.ArgumentParser(description='Call instruments from API and create static universes.')
    parser.add_argument('--dir', help='folder dir of static_uni.csv', type=str, default='data/')
    parser.add_argument('--remove','-r', help='remove if exists', type=bool, default=False)
    args = parser.parse_args()

    top_uni = ['']
    bot_uni = ['']

    df = pd.DataFrame( columns=['startdate','ticker','exchange'] )
    FullUni = LoadInstruments(df,top_uni,bot_uni)
    inst = FullUni.run(args.remove,args.dir)
    inst = get_country_uni(inst,country_settings)
    inst = get_base_currency(inst)
    inst = create_int(inst)

    filename = args.dir + 'static_uni.csv'
    if os.path.isfile(filename):
        os.remove(filename)
    inst.to_csv(filename)
