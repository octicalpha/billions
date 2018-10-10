
# https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/list-of-technical-indicators/


import pandas as pd
import numpy as np


class Technical_Indicator(object):

    def __init__(self, ohlcv, pnl_path=None):
        self._initialize(ohlcv, pnl_path)
        
    def _initialize(self, ohlcv, pnl_path):
                
        self._ohlcv = ohlcv
        self.METADATA = METADATA
        if pnl_path:
            self.pnl_df = pd.read_csv(pnl_path, index_col=0)
        
        class Obj(object):
            def __init__(self):
                pass
            
        get = Obj()
        for key in METADATA:
            func = getattr(self, "_get_"+key)
            setattr(get, key, func)

        describe = Obj()
        for key in METADATA:
            setattr(describe, key, METADATA[key])
            
        self.get = get
        self.describe = describe
        
    def _get_ohlcv_se(self, ticker):
        ohlcv = self._ohlcv
        df = ohlcv[ohlcv["ticker"] == ticker]
        return df["open"], df["high"], df["low"], df["close"], df["baseVolume"]
    
    
    def _get_ABANDS(self, ticker, window_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        upper_band = h*(1+4*(h-l)/(h+l))
        middle_band = c
        lower_band = l*(1-4*(h-l)/(h+l))
        
        upper_band = upper_band.rolling(window_n).mean()
        middle_band = middle_band.rolling(window_n).mean()
        lower_band = lower_band.rolling(window_n).mean()
        
        return upper_band, middle_band, lower_band
    
    def _get_AD(self, ticker):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)

        AD = ((((c-l)-(h-c))/(h-l))*v)
        AD = AD.fillna(0)
        AD = AD.expanding().sum()
        
        return AD
    
    def _get_ADX(self, ticker, smoothing_n=12):
        
        pDI, nDI = self._get_DMI(ticker, smoothing_n)
        
        DIdiff = (pDI-nDI).abs()
        DIsum = pDI+nDI
        DX = (DIdiff/DIsum*100).fillna(0)
        
        DX = DX.rolling(2).apply(lambda x: (x[0]*(smoothing_n-1)+x[1])/smoothing_n)
        ADX = DX
        
        return ADX
    
    def _get_AMA(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
    
    def _get_APO(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
    
    def _get_AR(self, ticker, window_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        period_n = window_n
        h_period_n = (h.rolling(period_n)
                      .apply(lambda x: len(x[x.argmax():])))
        l_period_n = (l.rolling(period_n)
                      .apply(lambda x: len(x[x.argmin():])))
        
        AroonUp = (period_n-h_period_n)/period_n*100
        AroonDown = (period_n-l_period_n)/period_n*100
        
        return AroonUp, AroonDown
    
    def _get_ARO(self, ticker):
        AroonUp, AroonDown = self._get_AR(ticker)
        AROSC = AroonUp-AroonDown
        return AROSC
    
    def _get_ATR(self, ticker, window_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        pc = c.rolling(2).apply(lambda x: x[0])
        TR = pd.concat([(h-l), (h-pc), (pc-l)], axis=1).max(axis=1)
        ATR = TR.rolling(window_n).mean()
        
        return ATR
    
    def _get_AVOL(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
    
    def _get_BAVOL(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
    
    def _get_BBANDS(self, ticker, window_n=12, y=2):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        std = c.rolling(window_n).std()
        middle_band = c.rolling(window_n).mean()
        upper_band = middle_band+(y*std)
        lower_band = middle_band-(y*std)
        
        return upper_band, middle_band, lower_band
    
    def _get_BVA(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_BVOL(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_BW(self, ticker, window_n=12, y=2):
        return self._get_BBANDS(ticker, window_n, y)
    
    def _get_CCI(self, ticker, window_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        H = c.rolling(window_n).max().fillna(0)
        L = c.rolling(window_n).min().fillna(0)
        C = c
        M = (H+L+C)/3
        A = M.rolling(window_n).mean().fillna(0)
        D = (M-A).abs().std()
        CCI = (M-A)/(0.015*D)

        return CCI
    
    def _get_CMO(self, ticker):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        def get_CMO_scalar(a):
            se = pd.Series(a).rolling(2).apply(lambda x: x[1]-x[0])
            PosSum = se[se > 0].sum()
            NegSum = se[se < 0].abs().sum()
            CMO = (PosSum-NegSum)/(PosSum+NegSum)*100
            return CMO
        
        CMO = c.expanding().apply(lambda x: get_CMO_scalar(x))
        return CMO
    
    def _get_DEMA(self, ticker, n=12):
        
        EMA = self._get_EMA(ticker, n)
        K = 2.0/(n+1)
        len_ = len(EMA)
        K_se = pd.Series([(1-K)**(len_-i-1) for i in range(len_)])
        
        DEMA = EMA*2-EMA.expanding().apply(
            lambda x: (K_se[-len(x):]*x*K).sum()
        )
        
        return DEMA
        
    def _get_DMI(self, ticker, smoothing_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        pDM = (h.rolling(2)
               .apply(lambda x: x[1]-x[0])
               .apply(lambda x: x if x > 0 else 0)
               .fillna(0))
        nDM = (l.rolling(2)
               .apply(lambda x: x[0]-x[1])
               .apply(lambda x: x if x > 0 else 0)
               .fillna(0))
        pDM[pDM < nDM] = 0
        nDM[nDM < pDM] = 0
        TR = self._get_ATR(ticker, 1)
        
        pDM = (pDM.rolling(2)
               .apply(lambda x: (x[0]-x[0]/smoothing_n)+x[1])
               .fillna(0))
        nDM = (nDM.rolling(2)
               .apply(lambda x: (x[0]-x[0]/smoothing_n)+x[1])
               .fillna(0))
        TR = (TR.rolling(2)
              .apply(lambda x: (x[0]-x[0]/smoothing_n)+x[1])
              .fillna(0))
        
        pDI = (pDM/TR*100).fillna(0)
        nDI = (nDM/TR*100).fillna(0)
        
        return pDI, nDI
    
    def _get_EMA(self, ticker, window_n=12, series=None):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        se = c if type(series) == None.__class__ else series
        K, l = 2.0/(window_n+1), len(se)
        w = np.array([(1-K)**(l-1-i) for i in range(l)])
        EMA = se.expanding().apply(
            lambda x: x.dot(w[-len(x):])*K
        )
        return EMA
    
    def _get_FILL(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_ICH(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_KC(self, ticker, window_n=12, y=1):

        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        ATR = self._get_ATR(ticker, window_n)
        middle_line = ((h+l+c)/3).rolling(window_n).mean()
        upper_band = middle_line+y*ATR
        lower_band = middle_line-y*ATR
        
        return upper_band, middle_line, lower_band
    
    def _get_LR(self, ticker, window_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        def get_m_with_b(y):
            x_sum = x_pts.sum()
            y_sum = y.sum()
            m = (
                (window_n*(y*x_pts).sum()-x_sum*y_sum)
                 /(window_n*(x_pts**2).sum()-x_sum**2)
            )
            b.append((y_sum-m*x_sum)/window_n)
            return m

        x_pts = np.array(range(window_n))
        b = [0]*(window_n-1)
        m_se = c.rolling(window_n).apply(get_m_with_b)
        b_se = pd.Series(b, index=m_se.index)
        
        return m_se, b_se
    
    def _get_LRA(self, ticker, window_n=12):
        m_se, b_se = self._get_LR(ticker, window_n)
        LRA = np.arctan(m_se)/np.pi*180
        return LRA
    
    def _get_LRI(self, ticker, window_n=12):
        m_se, b_se = self._get_LR(ticker, window_n)
        LRI = b_se
        return LRI
    
    def _get_LRM(self, ticker, window_n=12):
        m_se, b_se = self._get_LR(ticker, window_n)
        LRM = m_se
        return LRM
    
    def _get_MACD(self, ticker, small_window_n=12, big_window_n=36):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        FastMA = c.rolling(small_window_n).mean()
        SlowMA = c.rolling(big_window_n).mean()
        MACD = FastMA-SlowMA
        
        return MACD
    
    def _get_MAX(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        MAX = c.rolling(window_n).max()
        return MAX
    
    def _get_MFI(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_MIDPNT(self, ticker, window_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        hc = c.rolling(window_n).max()
        lc = c.rolling(window_n).min()
        
        MIDPNT = (hc-lc).expanding().mean()
        return MIDPNT
    
    def _get_MIDPRI(self, ticker, window_n=12):
        
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        hh = h.rolling(window_n).max()
        ll = l.rolling(window_n).min()
        
        MIDPRI = (hh-ll).rolling(window_n).mean()
        return MIDPRI
    
    def _get_MIN(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        MIN = c.rolling(window_n).min()
        return MIN
    
    def _get_MINMAX(self, ticker, window_n=12):
        MIN = self._get_MIN(ticker, window_n)
        MAX = self._get_MAX(ticker, window_n)
        return MIN, MAX
    
    def _get_MOM(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        MOM = c.rolling(window_n).apply(lambda x: x[-1]-x[0])
        return MOM
    
    def _get_NATR(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        ATR = self._get_ATR(ticker, window_n)
        NATR = ATR/c*100
        return NATR
    
    def _get_OBV(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        cc = c.rolling(2).apply(lambda x: x[1]-x[0])
        v = v.copy()
        v[cc < 0] *= -1
        
        OBV = v.cumsum()
        return OBV
    
    def _get_PC(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        upper_band = c.rolling(window_n).max()
        lower_band = c.rolling(window_n).min()
        return upper_band, lower_band
    
    def _get_PLT(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_PPO(self, ticker, s_window_n=12, l_window_n=36):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        FastMA = c.rolling(s_window_n).mean()
        SlowMA = c.rolling(l_window_n).mean()
        
        PPO = (FastMA-SlowMA)/SlowMA*100
        return PPO
    
    def _get_PVT(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        intPVT = c.rolling(2).apply(lambda x: (x[1]-x[0])/x[0])*v
        
        PVT = intPVT.cumsum()
        return PVT
    
    def _get_ROC(self, ticker, window_n=12):
        ROC = self._get_MOM(ticker, window_n)
        return ROC
    
    def _get_ROC100(self, ticker, window_n=12):
        ROC = self._get_ROC(ticker, window_n)
        ROC100 = ROC*100
        return ROC100
    
    def _get_ROCP(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        ROCP = c.rolling(window_n).apply(lambda x: x[-1]/x[0]-1)
        return ROCP
    
    def _get_RSI(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        pnl_df = self.pnl_df
        
    def _get_S_VOL(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        def get_se_for_each_term(i):
            return v[i:i+window_n].expanding().sum()
        
        S_VOL = pd.concat(
            [get_se_for_each_term(i*window_n)
             for i in range(len(v)//window_n+1)]
        )
        
        return S_VOL
    
    def _get_SAR(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_SAVOL(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_SBVOL(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_SMA(self, ticker, window_n=12, series=None):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        se = c if type(series) == None.__class__ else series
        SMA = se.rolling(window_n, 1).mean()
        return SMA
    
    def _get_STDDEV(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        STDDEV = c.rolling(window_n, 1).std()
        return STDDEV
    
    def _get_STOCH(self, ticker, k_window_n=12, d_window_n=12):
        fast_k, slow_k = self._get_STOCHF(ticker, k_window_n)
        slow_d = slow_k.rolling(d_window_n, 1).mean()
        return fast_k, slow_k, slow_d
    
    def _get_STOCHF(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        fast_k = (100*((c-l)/(h-l))
                  .rolling(window_n, 1)
                  .mean()
                  .fillna(0))
        fast_d = fast_k.rolling(window_n, 1).mean()
        return fast_k, fast_d
    
    def _get_T3(self, ticker, window_n=12, vFactor=0.7):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
        def get_GD(t):
            EMA1 = self._get_EMA(ticker, window_n, t)
            EMA2 = self._get_EMA(ticker, window_n, EMA1)
            return EMA1*(1+vFactor)-EMA2*vFactor
        
        T3 = get_GD(get_GD(get_GD(c)))
        return T3
    
    def _get_TEMA(self, ticker, window_n=12):
        EMA1 = self._get_EMA(ticker, window_n)
        EMA2 = self._get_EMA(ticker, window_n, EMA1)
        EMA3 = self._get_EMA(ticker, window_n, EMA2)
        TEMA = 3*EMA1-3*EMA2+EMA3
        return TEMA
    
    def _get_TRIMA(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        N = window_n+1
        Nm = int(round(window_n+1/2.0))
        SMAm = c.rolling(Nm, 1).mean()
        TRIMA = self._get_SMA(ticker, Nm, SMAm)
        return TRIMA
        
    def _get_TRIX(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        EMA1 = self._get_SMA(ticker, window_n)
        EMA2 = self._get_SMA(ticker, window_n, EMA1)
        EMA3 = self._get_SMA(ticker, window_n, EMA2)
        TRIX = EMA3.rolling(2).apply(lambda x: (x[1]-x[0])/x[0])
        return TRIX
    
    def _get_TSF(self, ticker, window_n=12):
        m_se, b_se = self._get_LR(ticker, window_n)
        return m_se, b_se
    
    def _get_TT_CVD(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
    
    def _get_ULTOSC(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_VAP(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_VOLUME(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        return v
    
    def _get_VOL_DELTA(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_VWAP(self, ticker, window_n=120):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        
    def _get_WILLR(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        highest_high = h.rolling(window_n, 1).max()
        lowest_low = l.rolling(window_n, 1).min()
        R = -100*(highest_high-c)/(highest_high-lowest_low)
        R = R.fillna(0)
        return R
    
    def _get_WMA(self, ticker, window_n=12):
        o, h, l, c, v = self._get_ohlcv_se(ticker)
        w = np.array([i+1 for i in range(window_n)])
        WMA = c.rolling(window_n, 1).apply(
            lambda x: x.dot(w[-len(x):])*len(x)*(len(x)+1)/2
        )
        return WMA
    
    def _get_WWS(self, ticker):
        o, h, l, c, v = self._get_ohlcv_se(ticker)


METADATA = {
    "ABANDS": """
ACCELERATION BANDS (ABANDS)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/acceleration-bands-abands/
""",
    "AD": """
ACCUMULATION/DISTRIBUTION (AD)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/accumulationdistribution-ad/
""",
    "ADX": """
AVERAGE DIRECTIONAL MOVEMENT (ADX)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/average-directional-movement-adx/
""",
    "AMA": """
ADAPTIVE MOVING AVERAGE (AMA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/adaptive-moving-average-ama/
""",
    "APO": """
ABSOLUTE PRICE OSCILLATOR (APO)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/absolute-price-oscillator-apo/
""",
    "AR": """
AROON (AR)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/aroon-ar/
""",
    "ARO": """
AROON OSCILLATOR (ARO)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/aroon-oscillator-aro/
""",
    "ATR": """
AVERAGE TRUE RANGE (ATR)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/average-true-range-atr/
""",
    "AVOL": """
VOLUME ON THE ASK (AVOL)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/volume-on-the-ask-avol/
""",
    "BAVOL": """
VOLUME ON THE BID AND ASK (BAVOL)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/volume-on-the-bid-and-ask-bavol/
""",
    "BBANDS": """
BOLLINGER BAND (BBANDS)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/bollinger-band-bbands/
""",
    "BVA": """
BAR VALUE AREA (BVA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/bar-value-area-bva/
""",
    "BVOL": """
BID VOLUME (BVOL)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/bid-volume-bvol/
""",
    "BW": """
BAND WIDTH (BW)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/band-width-bw/
""",
    "CCI": """
COMMODITY CHANNEL INDEX (CCI)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/commodity-channel-index-cci/
""",
    "CMO": """
CHANDE MOMENTUM OSCILLATOR (CMO)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/chande-momentum-oscillator-cmo/
""",
    "DEMA": """
DOUBLE EXPONENTIAL MOVING AVERAGE (DEMA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/double-exponential-moving-average-dema/
""",
    "DMI": """
DIRECTIONAL MOVEMENT INDICATORS (DMI)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/directional-movement-indicators-dmi/
""",
    "EMA": """
EXPONENTIAL (EMA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/exponential-ema/
""",
    "FILL": """
FILL INDICATOR (FILL)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/fill-indicator-fill/
""",
    "ICH": """
ICHIMOKU (ICH)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/ichimoku-ich/
""",
    "KC": """
KELTNER CHANNEL (KC)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/keltner-channel-kc/
""",
    "LR": """
LINEAR REGRESSION (LR)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/linear-regression-lr/
""",
    "LRA": """
LINEAR REGRESSION ANGLE (LRA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/linear-regression-angle-lra/
""",
    "LRI": """
LINEAR REGRESSION INTERCEPT (LRI)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/linear-regression-intercept-lri/
""",
    "LRM": """
LINEAR REGRESSION SLOPE (LRM)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/linear-regression-slope-lrm/
""",
    "MACD": """
MOVING AVERAGE CONVERGENCE DIVERGENCE (MACD)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/moving-average-convergence-divergence-macd/
""",
    "MAX": """
MAX (MAX)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/max-max/
""",
    "MFI": """
MONEY FLOW INDEX (MFI)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/money-flow-index-mfi/
""",
    "MIDPNT": """
MIDPOINT (MIDPNT)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/midpoint-midpnt/
""",
    "MIDPRI": """
MIDPRICE (MIDPRI)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/midprice-midpri/
""",
    "MIN": """
MIN (MIN)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/min-min/
""",
    "MINMAX": """
MINMAX (MINMAX)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/minmax-minmax/
""",
    "MOM": """
MOMENTUM (MOM)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/momentum-mom/
""",
    "NATR": """
NORMALIZED AVERAGE TRUE RANGE (NATR)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/normalized-average-true-range-natr/
""",
    "OBV": """
ON BALANCE VOLUME (OBV)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/on-balance-volume-obv/
""",
    "PC": """
PRICE CHANNEL (PC)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/price-channel-pc/
""",
    "PLT": """
PLOT (PLT)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/plot-plt/
""",
    "PPO": """
PERCENT PRICE OSCILLATOR (PPO)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/percent-price-oscillator-ppo/
""",
    "PVT": """
PRICE VOLUME TREND (PVT)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/price-volume-trend-pvt/
""",
    "ROC": """
RATE OF CHANGE (ROC)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/rate-of-change-roc/
""",
    "ROC100": """
RATE OF CHANGE (ROC100)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/rate-of-change-roc100/
""",
    "ROCP": """
RATE OF CHANGE (ROCP)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/rate-of-change-rocp/
""",
    "RSI": """
RELATIVE STRENGTH INDICATOR (RSI)
[DESCRIPTION]
""",
    "S_VOL": """
SESSION VOLUME (S_VOL)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/session-volume-svol/
""",
    "SAR": """
PARABOLIC SAR (SAR)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/parabolic-sar-sar/
""",
    "SAVOL": """
SESSION CUMULATIVE ASK (SAVOL)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/session-cumulative-ask-savol/
""",
    "SBVOL": """
SESSION CUMULATIVE BID (SBVOL)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/session-cumulative-bid-sbvol/
""",
    "SMA": """
SIMPLE MOVING AVERAGE (SMA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/simple-moving-average-sma/
""",
    "STDDEV": """
STANDARD DEVIATION (STDDEV)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/standard-deviation-stddev/
""",
    "STOCH": """
STOCHASTIC (STOCH)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/stochastic-stoch/
""",
    "STOCHF": """
STOCHASTIC FAST (STOCHF)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/stochastic-fast-stochf/
""",
    "T3": """
T3 (T3)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/t3-t3/
""",
    "TEMA": """
TRIPLE EXPONENTIAL MOVING AVERAGE (TEMA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/triple-exponential-moving-average-tema/
""",
    "TRIMA": """
TRIANGULAR MOVING AVERAGE (TRIMA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/triangular-moving-average-trima/
""",
    "TRIX": """
TRIPLE EXPONENTIAL MOVING AVERAGE OSCILLATOR (TRIX)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/triple-exponential-moving-average-oscillator-trix/
""",
    "TSF": """
TIME SERIES FORECAST (TSF)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/time-series-forecast-tsf/
""",
    "TT_CVD": """
TT CUMULATIVE VOL DELTA (TT CVD)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/tt-cumulative-vol-delta-tt-cvd/
""",
    "ULTOSC": """
ULTIMATE OSCILLATOR (ULTOSC)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/ultimate-oscillator-ultosc/
""",
    "VAP": """
VOLUME AT PRICE (VAP)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/volume-at-price-vap/
""",
    "VOLUME": """
VOLUME (VOLUME)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/volume-volume/
""",
    "VOL_DELTA": """
VOLUME DELTA (VOL DELTA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/volume-delta-vol/
""",
    "VWAP": """
VOLUME WEIGHTED AVERAGE PRICE (VWAP)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/volume-weighted-average-price-vwap/
""",
    "WILLR": """
WILLIAMS % R (WILLR)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/williams-r-willr/
""",
    "WMA": """
WEIGHTED MOVING AVERAGE (WMA)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/weighted-moving-average-wma/
""",
    "WWS": """
WELLES WILDER'S SMOOTHING AVERAGE (WWS)
[DESCRIPTION]
https://www.tradingtechnologies.com/help/x-study/technical-indicator-definitions/welles-wilders-smoothing-average-wws/
"""
}
