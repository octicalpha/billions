import numpy as np
import pandas as pd
import cvxopt as opt
from cvxopt import blas, solvers

def optimal_portfolio(returns,N):
    solvers.options['show_progress'] = False
    n = len(returns)
    returns = np.asmatrix(returns)
    
    mus = [10**(5.0 * t/N - 1.0) for t in range(N)]
    
    # Convert to cvxopt matrices
    S = opt.matrix(np.cov(returns))
    pbar = opt.matrix(np.mean(returns, axis=1))
    
    # Create constraint matrices
    G = -opt.matrix(np.eye(n))   # negative n x n identity matrix
    h = opt.matrix(0.0, (n ,1))
    A = opt.matrix(1.0, (1, n))
    b = opt.matrix(1.0)

    # Calculate efficient frontier weights using quadratic programming
    portfolios = [solvers.qp(mu*S, -pbar, G, h, A, b)['x']
                  for mu in mus]
    ## CALCULATE RISKS AND RETURNS FOR FRONTIER
    returns = [blas.dot(pbar, x) for x in portfolios]
    risks = [np.sqrt(blas.dot(x, S*x)) for x in portfolios]
    ## CALCULATE THE 2ND DEGREE POLYNOMIAL OF THE FRONTIER CURVE
    m1 = np.polyfit(returns,risks,2)
    x1 = np.sqrt(m1[2] / m1[0])
    # CALCULATE THE OPTIMAL PORTFOLIO
    #import pdb; pdb.set_trace()
    wt = solvers.qp(opt.matrix(x1 * S), -pbar, G, h, A, b)['x']
    return np.asarray(wt), returns, risks

def data_handler(pnl_dir):
    pnl_all = pd.DataFrame()
    for i in pnl_dir:
        pnl_temp = pd.read_csv(i, index_col=0)
        pnl_ret = pnl_temp.loc[:,'PnL'] # / pnl_temp.loc[:,'Booksize']
        pnl_ret = pnl_ret.rename(i.split('/')[-1][:-4])
        pnl_all = pd.concat([pnl_all,pnl_ret],axis=1)
    return pnl_all

def pos_handler(pos_dir):
    pos_all = pd.DataFrame()
    for i in pos_dir:
        pos_temp = pd.read_csv(i, index_col=0)
        pos_temp['name'] = i.split('/')[-1][:-8]
        pos_temp = pos_temp.fillna(0)
        pos_all = pos_all.append(pos_temp)
    return pos_all

def filter_file(file,date_today,backdays):
    output = file.loc[:str(date_today),:]
    return output.iloc[-backdays:,:]
    
def filter_uni(file,alpha):
    output = file
    for col in output.columns:
        if col not in alpha.columns:
            output.drop(col,axis=1)
    return output           

def min_weight(weights,mincut):
    weights_sum = weights[0].sum()
    for i in weights.index:
        if weights[0][i] < mincut:
            weights[0][i] = mincut
    sum_val = weights[0].sum()
    weights[0] = weights[0].divide(sum_val)
    return weights

def max_weight(weights,maxcut):
    weights_sum = weights[0].sum()
    for i in weights.index:
        if weights[0][i] > maxcut:
            weights[0][i] = maxcut
    sum_val = weights[0].sum()
    weights[0] = weights[0].divide(sum_val)
    return weights

def weight_calc(pos_all,alpha,weights,date_today):
    output = pd.DataFrame()
    pos_temp = pos_all.loc[str(date_today),:]
    pos_temp = pos_temp.fillna(0)
    for i in weights.index:
        pos_temp_sub = pos_temp[pos_temp['name'].str.contains(i)]
        pos_temp_sub = pos_temp_sub.drop(['name'],axis=1)
        calc_pos = weights[0][i] * pos_temp_sub
        output = pd.concat([output,calc_pos],axis=0)
    output = output.sum(axis=0)
    return output

def get_weights(pnl_dir,pos_dir,alpha,rebalance,max_weight_value,backdays=288):
    pnl_all_out = data_handler(pnl_dir)
    pos_all_out = pos_handler(pos_dir)
    alpha_temp = pd.DataFrame()
    for i,row in alpha.iterrows():
        if str(i)[-8:] in rebalance:
            pnl_filter = filter_file(pnl_all_out,i,backdays)
            weights, _, _ = optimal_portfolio(pnl_filter.T.as_matrix(),backdays)
        weights = pd.DataFrame(weights)
        weights = max_weight(weights,max_weight_value)
        weights.index = pnl_all_out.columns.str[:-4]
        #print i, weights
        output = weight_calc(pos_all_out,alpha,weights,i)
        #import pdb; pdb.set_trace()
        output = output.rename(i)
        output = pd.DataFrame(output)
        alpha_temp = pd.concat([alpha_temp,output.T],axis=0)
    return alpha_temp