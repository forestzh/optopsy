from .option_queries import *
import pandas as pd

leg_cols = [f[0] for f in fields if f[3] == 'leg']
common_cols = [f[0] for f in fields if f[3] == 'common']


def _apply_ratio(data, ratio):
    return pd.concat([data.loc[:, common_cols],
                      data.loc[:, leg_cols] * ratio], axis=1)


def _collapse_values():
    pass


def _join_legs(legs):
    on=['quote_date', 'option_type', 'expiration']
    return reduce(lambda l, r: pd.merge(l, r, on=on), legs)
    

def _create_legs(data, legs):
    def _create_leg(leg):
        return (
            data
            .pipe(opt_type, option_type=leg[0])
            .pipe(nearest, 'delta', leg[1])
            .pipe(lte, 'dte', leg[2])
            .pipe(_apply_ratio, ratio=leg[3])
        ).reset_index(drop=True)
    return [_create_leg(legs[l]) for l in range(0, len(legs))]


def single(data, leg):
    return _create_legs(data, leg)[0]


def vertical(data, l):
    legs = _create_legs(data, l)
    if len(legs) != 2:
        raise ValueError("Invalid legs defined for vertical strategy!")
    else:
        return (
            legs
            .pipe(_join_legs)
            .pipe(collapse_values, l)
        )
       