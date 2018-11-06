from .option_queries import *
from functools import reduce
import pandas as pd


leg_cols = [f[0] for f in fields if f[3] == 'leg']
common_cols = [f[0] for f in fields if f[3] == 'common']


def _apply_ratio(data, ratio, method='concat'):
    if method == 'concat' and ratio < 0:
        # swap bid and ask values, to be used in concat_legs algorithm
        pass
    return pd.concat([data.loc[:, common_cols],
                      data.loc[:, leg_cols] * ratio], axis=1)


def _collapse_values(spread, legs):
    print(spread)
    return spread


# Merge method #1
def _join_legs(legs):
    on = ['quote_date', 'option_type', 'expiration']
    suffixes = [f"_{i+1}" for i in range(len(legs))]
    return reduce(lambda l, r: pd.merge(l, r, on=on, suffixes=suffixes), legs)


# Merge method #2
def _concat_legs(legs):
    sort_by = ['quote_date', 'option_type', 'expiration', 'strike']
    return (
        reduce(lambda l, r: pd.concat([l, r]), legs)
        .sort_values(sort_by)
        .groupby(sort_by[:-1]).sum()
    )


def _create_legs(data, legs, method='join'):
    def _create_leg(leg):
        return (
            data
            .pipe(opt_type, option_type=leg[0])
            .pipe(nearest, 'delta', leg[1])
            .pipe(lte, 'dte', leg[2])
            .pipe(_apply_ratio, ratio=leg[3], method=method)
        ).reset_index(drop=True)
    return [_create_leg(legs[l]) for l in range(0, len(legs))]


def singles(data, leg):
    return _create_legs(data, leg)[0]


def spreads(data, legs):
    spread = _create_legs(data, legs)
    if len(spread) < 2:
        raise ValueError("Invalid legs defined for vertical strategy!")
    else:
        # return _join_legs(spread).pipe(_collapse_values, legs)
        return _concat_legs(spread).pipe(_collapse_values, legs)