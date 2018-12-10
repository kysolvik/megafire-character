#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""Quick script to clean and deduplicate megafire tweet csvs

Notes:
    A few of the csv files in the megafires dataset had date formats with +0000
    instead of "UTC". That made it hard when reading into pandas.
    This script just converts the ones that need to be converted.

    ALSO removes tweets without timestamps and deduplicates everything

    ALSO a few of the csv files have weird format destroyers that mess things up
    after saving. To get around this, the script just copies the original csvs
    to the new directory.
    """


import argparse
import glob
import pandas as pd
import os
import shutil


def argparse_init():
    """Prepare ArgumentParser for inputs."""

    p = argparse.ArgumentParser(
            description='Clean and dedup megafire tweet CSVs',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('input_dir',
                   help='Dir containing input csvs. Will glob all csvs inside.',
                   type=str)
    p.add_argument('output_dir',
                   help='Path to output directory. Will fail if not exists.',
                   type=str)

    return p


def main():

    parser = argparse_init()
    args = parser.parse_args()

    if not os.path.isdir(args.output_dir):
        raise ValueError('Output Dir does not exist. Create and try again.')

    csv_list = glob.glob('{}/*.csv'.format(args.input_dir))

    for csv in csv_list:
        output_path = csv.replace(args.input_dir, args.output_dir)
        # If it's waldo, wallow, or yarnell hill, just copy file
        noreplace = ['2012-waldo-canyon-fire-co.csv',
                     '2013-yarnell-hill-az.csv',
                     '2011-wallow-az.csv']
        fn = os.path.basename(csv)
        if fn in noreplace:
            shutil.copyfile(csv, output_path)
        else:
            df = pd.read_csv(csv)
            nan_timestamps = df.Timestamp.isna()
            total_nans += nan_timestamps.sum()
            df = df.loc[~nan_timestamps]
            new_ts = [str(t).replace('+0000', 'UTC') for t in df.Timestamp.values]
            df.Timestamp = new_ts
            df.drop_duplicates('Tweet Id', keep='last', inplace=True)
            df.to_csv(output_path, index=False)


if __name__ == '__main__':
    main()
