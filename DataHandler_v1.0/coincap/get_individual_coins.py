import time
from get_data_fns import store_it,get_coins,get_page

if __name__ == '__main__':
    coins = get_coins()
    while True:
        for coin in get_coins():
            store_it('page',lambda: get_page(coin))
        time.sleep(2.5)

