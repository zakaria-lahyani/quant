from typing import Dict, Type

from app.indicators.incremental.aroon import Aroon
from app.indicators.incremental.adx import ADX
from app.indicators.incremental.bollinger_bands import BollingerBands
from app.indicators.incremental.ichimoku import Ichimoku
from app.indicators.incremental.keltner_channel import KeltnerChannel
from app.indicators.incremental.stochastic_rsi import StochasticRSI
from app.indicators.incremental.supertrend import Supertrend
from app.indicators.incremental.ultimate_rsi import UltimateRsi
from app.indicators.incremental.obv import OBV
from app.indicators.incremental.macd import MACD
from app.indicators.incremental.atr import ATR
from app.indicators.incremental.rsi import RSI
from app.indicators.incremental.sar import SAR
from app.indicators.incremental.ema import EMA
from app.indicators.incremental.sma import SMA
from app.indicators.incremental.rma import RMA

INDICATOR_CLASSES: Dict[str, Type] = {
    'aroon': Aroon,
    'adx': ADX,
    'ursi': UltimateRsi,
    'bb': BollingerBands,
    'keltner': KeltnerChannel,
    'atr': ATR,
    'rsi': RSI,
    'sar': SAR,
    'stochrsi': StochasticRSI,
    'supertrend': Supertrend,
    'macd': MACD,
    'ichimoku': Ichimoku,
    'obv': OBV,
    'ema': EMA,
    'sma': SMA,
    'rma': RMA,
}

DEFAULT_PARAMETERS: Dict[str, dict] = {
    'ursi': {'src': 'close', 'length': 14, 'smooth_length': 14},
    'bb': {'window': 20, 'num_std_dev': 2},
    'keltner': {'ema_window': 20, 'atr_window': 10, 'multiplier': 2},
    'atr': {'window': 14},
    'rsi': {'period': 14, "signal_period":14},
    'sar': {},
    'stochrsi': {},
    'supertrend': {'period': 10, 'multiplier': 3},
    'macd': {'fast': 12, 'slow': 26, 'signal': 9},
    'ichimoku': {'tenkan_period': 9, 'kijun_period': 26, 'senkou_b_period': 52, 'chikou_shift': 26},
    'adx': {'period': 14},
    'obv': {'period': 14},
    'aroon': {'period': 14},
    'ema': {'period': 14},
    'sma': {'period': 14},
    'rma': {'period': 14},
}

INDICATOR_CONFIG = {
    'ursi': {
        'inputs': lambda row: (row,),
        'bulk_inputs': lambda df: (df,),
        'outputs': lambda name: [name, f'signal_{name}']
    },
    'bb': {
        'inputs': lambda row: (row['close'],),
        'bulk_inputs': lambda df: (df['close'],),
        'outputs': lambda name: [f'{name}_upper', f'{name}_middle', f'{name}_lower', f'{name}_percent_b']
    },
    'atr': {
        'inputs': lambda row: (row['high'], row['low'], row['close']),
        'bulk_inputs': lambda df: (df['high'], df['low'], df['close']),
        'outputs': lambda name: [name]
    },
    'rsi': {
        'inputs': lambda row: (row['close'],),
        'bulk_inputs': lambda df: (df['close'],),
        'outputs': lambda name: [name, f'signal_{name}']
    },
    'sar': {
        'inputs': lambda row: (row['high'], row['low']),
        'bulk_inputs': lambda df: (df['high'], df['low']),
        'outputs': lambda name: [name]
    },
    'stochrsi': {
        'inputs': lambda row: (row['close'],),
        'bulk_inputs': lambda df: (df['close'],),
        'outputs': lambda name: [f'{name}_k', f'{name}_d']
    },
    'supertrend': {
        'inputs': lambda row: (row['high'], row['low'], row['close']),
        'bulk_inputs': lambda df: (df['high'], df['low'], df['close']),
        'outputs': lambda name: [name, f'trend_{name}']
    },
    'macd': {
        'inputs': lambda row: (row['close'],),
        'bulk_inputs': lambda df: (df['close'],),
        'outputs': lambda name: [name, f'{name}_signal', f'{name}_hist']
    },
    'ichimoku': {
        'inputs': lambda row: (row['high'], row['low'], row['close']),
        'bulk_inputs': lambda df: (df['high'], df['low'], df['close']),
        'outputs': lambda name: [f'{name}_tenkan', f'{name}_kijun', f'{name}_senkou_a',
                                 f'{name}_senkou_b', f'{name}_chikou', f'{name}_cloud']
    },
    'adx': {
        'inputs': lambda row: (row['high'], row['low'], row['close']),
        'bulk_inputs': lambda df: (df['high'], df['low'], df['close']),
        'outputs': lambda name: [name, f'{name}_plus_di', f'{name}_minus_di']
    },
    'obv': {
        'inputs': lambda row: (row['close'], row['tick_volume']),
        'bulk_inputs': lambda df: (df['close'], df['tick_volume']),
        'outputs': lambda name: [name, f'{name}_ema']
    },
    'aroon': {
        'inputs': lambda row: (row['high'], row['low']),
        'bulk_inputs': lambda df: (df['high'], df['low']),
        'outputs': lambda name: [f'{name}_up', f'{name}_down']
    },
    'keltner': {
        'inputs': lambda row: (row['high'], row['low'], row['close']),
        'bulk_inputs': lambda df: (df['high'], df['low'], df['close']),
        'outputs': lambda name: [f'{name}_upper', f'{name}_middle', f'{name}_lower', f'{name}_percent_b']
    },
    'ema': {
        'inputs': lambda row: (row['close'],),
        'bulk_inputs': lambda df: (df['close'],),
        'outputs': lambda name: [name]
    },
    'sma': {
        'inputs': lambda row: (row['close'],),
        'bulk_inputs': lambda df: (df['close'],),
        'outputs': lambda name: [name]
    },
    'rma': {
        'inputs': lambda row: (row['close'],),
        'bulk_inputs': lambda df: (df['close'],),
        'outputs': lambda name: [name]
    },
}

