from get_data_fns import continuously_store,get_front

if __name__ == '__main__':
    continuously_store('front',get_front,waittime=2.5)

