import numpy as np
import pandas as pd

def apply_delay(data,delay):
    """
    applies delay by variable delay to avoid forward-bias
    """
    output = data.shift(delay)
    return output.iloc[delay:]

def apply_date_filter(data,startdate,enddate):
    """
    applies delay by variable delay to avoid forward-bias
    """
    mask = (data.index >= startdate) & (data.index <= enddate)
    output = data.loc[mask]
    output = output[~output.index.duplicated(keep='first')]
    return output

def apply_norm(alpha, long_only=True, fill_na=True):
    """
    normalizes alpha
    """
    #import pdb; pdb.set_trace()     
    sum_val = alpha.abs().sum(axis=1)
    output = alpha.divide(sum_val, axis='index')
    """
    try:
        output = alpha.divide(sum_val,axis='index')
    except:
        output = alpha
        for index, row in alpha.iterrows():
            try:
                #import pdb; pdb.set_trace()
                output.loc[index] = alpha.loc[index].divide(sum_val.loc[index],axis='index')
            except:
                output.loc[index] = 0.0    
    """      
    if long_only:
        output[output < 0] = 0
        sum_val = output.abs().sum(axis=1)
        output = output.divide(sum_val,axis='index')
    if fill_na:
        output = output.fillna(0)
    return output

def apply_scale_to_book(alpha,booksize,bookfloat,long_only):
    """
    scale alpha to booksize
    """
    if bookfloat:
        return alpha
    else:
        output = apply_norm(alpha,long_only,fill_na=True)
        return output * booksize

def apply_truncate_book(alpha,booksize):
    """
    scale alpha to booksize
    """
    for index, row in alpha.iterrows():
        sum_val = alpha.loc[index].abs().sum()
        if sum_val > booksize:
            alpha.loc[index] = alpha.loc[index] / (sum_val) * booksize
    return alpha

def apply_truncate_coin(alpha,col,size):
    """
    scale coin to size
    """
    if alpha.loc[:,col].abs() > size:
        alpha.loc[:,col] = np.sign(alpha.loc[:,col]) * booksize
    return alpha

def apply_to_coin(alpha,price,max_size,BTC_col):
    """
    scale weights to coin size (only USD, USDT base)
    """
    output = alpha
    for col in output:
        #import pdb; pdb.set_trace()
        if col.split('-')[0] in 'BTC':
            temp = max_size * output.loc[:,col]
            output.loc[:,col] = temp.round(2)
        else:
            temp = max_size * output.loc[:,col] * (price.loc[:,BTC_col] / price.loc[:,col])
            #temp = max_size * output.loc[:,col] * (price.loc[:,col]/price.loc[:,BTC_col])
            output.loc[:,col] = temp.round(1)
    return alpha

def apply_truncate(alpha,booksize,percent):
    """
    truncate alpha
    """
    for index, row in alpha.iterrows():
        for i in alpha.columns:
            if np.abs(alpha.loc[index,i]) > (percent*booksize) and not np.isnan(alpha.loc[index,i]):
                alpha.loc[index,i] = alpha.loc[index,i] * ( (percent*booksize)/np.abs(alpha.loc[index,i]) )
    return alpha

def apply_short_only(alpha,exch):
    """
    scale alpha to booksize
    """
    output = alpha
    for i,row in output.iterrows():
        for col in output:
            if not np.isnan(row[col]):
                col_split = col.split('-')
                if exch in col_split[2] and row[col] > 0.0:
                    row[col] = 0.0
    return output

def apply_to_matrix(df,field,col,startdate,enddate,interval):
    """
    df - df to filter
    field - column or field to use
    col - column to with names
    """
    df_sub = df.loc[:,[field,col]]
    df_index = df_sub.index.unique()
    df_col = df_sub[col].unique()
    output = apply_date_filter(pd.DataFrame(index=df_index.sort_values(),columns=df_col),startdate,enddate)
    for i in output.columns:
        df_filter = apply_date_filter(df_sub[df_sub[col].str.contains(i)],startdate,enddate)
        output[i] = df_filter[field]
    interval_min = str(interval) + 'min'
    idx = pd.date_range(startdate,enddate,freq=interval_min)
    output = output.reindex(idx, fill_value=np.nan)

    return output

def apply_only_set_intervals(df,startdate,enddate,freq):
    """
    Reindex ohlcv to show only intervals within frequency
    """
    interval_min = str(freq) + 'min'
    idx = pd.date_range(startdate,enddate,freq=interval_min)
    import pdb; pdb.set_trace()
    output = df.reindex(idx, fill_value=np.nan)
    return output

def apply_shift_to_pos(alpha):
    """
    shift all values to positive
    """
    min_alpha = alpha.min(axis=1)
    output = alpha.subtract(min_alpha,axis='index')
    return output

def apply_shift_per_side_exch(alpha,exch_long,exch_short,booksize):
    """
    shift all values to positive
    """
    output = alpha
    for i,row in output.iterrows():
        norm_sum_long = 0
        norm_sum_short = 0
        temp_min = np.inf 
        temp_max = -np.inf
        for col in output:
            col_split = col.split('-')
            if not np.isnan(row[col]):
                if exch_long in col_split[2] and temp_min > row[col]:
                    temp_min = row[col]
                if exch_short in col_split[2] and temp_max < row[col]:
                    temp_max = row[col]
        for col in output:
            col_split = col.split('-')
            if exch_long in col_split[2]:
                row[col] += np.abs(temp_min)
                if not np.isnan(row[col]):
                    norm_sum_long += np.abs(row[col])
            if exch_short in col_split[2]:
                row[col] -= np.abs(temp_max)
                if not np.isnan(row[col]):
                    norm_sum_short += np.abs(row[col])
            #print np.abs(row[col])
        for col in output:
            col_split = col.split('-')
            if exch_long in col_split[2]:
                row[col] /= norm_sum_long
                row[col] *= booksize
            if exch_short in col_split[2]:
                row[col] /= norm_sum_short
                row[col] *= booksize
        output.loc[i] = row
    
    return output

def apply_exch_booksize_trunc(alpha,exch1,exch2,trunc,booksize):
    """
    apply booksize trunc per exchange
    """
    output = alpha
    for i,row in output.iterrows():
        exch1_size = 0
        exch2_size = 0
        for col in output:
            col_split = col.split('-')
            if not np.isnan(row[col]):
                if exch1 in col_split[2]:
                    exch1_size += row[col]
                elif exch2 in col_split[2]:
                    exch2_size += row[col]
        if exch1_size > (trunc * booksize):
            size_reduction = (trunc * booksize) / exch1_size
            for col in output:
                col_split = col.split('-')
                if exch1 in col_split[2]:
                    row[col] *= size_reduction
        if exch2_size > (trunc * booksize):
            size_reduction = (trunc * booksize) / exch2_size
            for col in output:
                col_split = col.split('-')
                if exch1 in col_split[2]:
                    row[col] *= size_reduction
        output.loc[i] = row

    return output

def apply_shift_to_pos_exch_long(alpha,exch,booksize):
    """
    shift all values to positive
    """
    output = alpha
    for i,row in output.iterrows():
        norm_sum = 0
        temp_min = np.inf 
        for col in output:
            col_split = col.split('-')
            if exch in col_split[2] and temp_min > row[col]:
                temp_min = row[col]
        for col in output:
            col_split = col.split('-')
            if exch in col_split[2]:
                row[col] += np.abs(temp_min)
                norm_sum += np.abs(row[col])
        for col in output:
            col_split = col.split('-')
            if exch in col_split[2]:
                row[col] /= norm_sum
                row[col] *= booksize
        output.loc[i] = row
    
    return output

def apply_shift_to_pos_exch_short(alpha,exch,booksize):
    """
    shift all values to positive
    """
    output = alpha
    for i,row in output.iterrows():
        norm_sum = 0
        temp_max = -np.inf
        for col in output:
            col_split = col.split('-')
            if exch in col_split[2] and temp_max < row[col]:
                temp_max = row[col]
        for col in output:
            col_split = col.split('-')
            if exch in col_split[2]:
                row[col] -= np.abs(temp_max)
                norm_sum += np.abs(row[col])
        for col in output:
            col_split = col.split('-')
            if exch in col_split[2]:
                row[col] /= norm_sum
                row[col] *= booksize
        output.loc[i] = row

    return output
        
def apply_zero_neg(alpha):
    """
    shift all negative values to zero
    """
    alpha[alpha < 0] = 0
    return alpha

def apply_filter_no_data_tickers(uni,data):
    """
    remove columns with no data
    """
    unique_col = data.columns.values
    output = uni
    for col in output:
        if col not in unique_col:
            del output[col]
    return output

def apply_ls_scale_to_book(alpha,booksize):
    for i,row in alpha.iterrows():
        alpha_temp = row
        pos_sum_val = alpha_temp.ix[alpha_temp > 0].abs().sum()
        neg_sum_val = alpha_temp.ix[alpha_temp < 0].abs().sum()
        for index in range(0,len(row)):
            if row[index] > 0:
                row[index] *= (booksize/2.0) / pos_sum_val
            elif row[index] < 0:
                row[index] *= (booksize/2.0) / neg_sum_val
        alpha.loc[i,:] = row
    return alpha
        
def apply_fill_check(alpha):
    prev_i = alpha.index[0]
    for i,row in alpha.iterrows():
        #import pdb; pdb.set_trace()
        if sum(alpha.loc[i,:].abs()) < 0.0000000000001:
            alpha.loc[i,:] = alpha.loc[prev_i,:]
        prev_i = i
    return alpha


def backfill(data):
    output = data
    prev = np.nan
    for col in output:
        for index, row in output.iterrows():
            if np.isnan(output[col][index]):
                output[col][index] = prev
            prev = output[col][index]
        prev = np.nan
    output = output.fillna(0)
    return output