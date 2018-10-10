from get_data_fns import continuously_store,get_global

if __name__ == '__main__':
    continuously_store('global',get_global,waittime=2.5)

