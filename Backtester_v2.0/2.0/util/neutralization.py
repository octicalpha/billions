import numpy as np
import pandas as pd

class Neutralization():
    def __init__(self,alpha):
        self.alpha = alpha

    def all_neutralize(self):
        """
        all_neutralize : neutralize all instruments
        """ 
        alpha_mean = self.alpha.mean(axis=1)
        output = self.alpha.subtract(alpha_mean,axis='index')
        return output

    def long_neutralize(self):
        """
        long_neutralize : neutralize long instruments
        """
        import pdb; pdb.set_trace()
        alpha_mean = self.alpha[self.alpha > 0].mean(axis=1)
        output = self.alpha[self.alpha > 0].subtract(alpha_mean,axis='index')
        return output

    def short_neutralize(self):
        """
        short_neutralize : neutralize short instruments
        """
        alpha_mean = self.alpha[self.alpha < 0].mean(axis=1)
        output = self.alpha[self.alpha < 0].subtract(alpha_mean,axis='index')
        return output

    def static_neutralize(self,type_col,uni_dir):
        """
        static_neutralize : neutralize instruments based on static grouping
        type_col = exchange, country, base, quote
        """
        uni = self.get_static_uni(uni_dir)
        uni = uni.set_index(['ticker'])
        type_temp = type_col + '_int'
        output = self.alpha
        #import pdb; pdb.set_trace()
        alpha_col = self.alpha.columns
        for i in uni.index:
            if i not in alpha_col:
                uni = uni.drop(i)
        unique_col = uni[:][type_temp].unique()
        sum_temp = pd.DataFrame(0,index=self.alpha.index,columns=unique_col)
        for i1 in unique_col:
            count = 0
            for i2 in uni.index:
                if i1 == uni[type_temp][i2]:
                    sum_temp.loc[:,i1] += output.loc[:,i2]
                    count += 1
            sum_temp.loc[:,i1] /= count
            for i2 in uni.index:
                if i1 == uni[type_temp][i2]:
                    output.loc[:,i2] -= sum_temp.loc[:,i1]
        #import pdb; pdb.set_trace()
        return output
    
    def get_static_uni(self,uni_dir):
        output = pd.read_csv(uni_dir, index_col=0)
        return output