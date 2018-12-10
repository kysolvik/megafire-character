#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Converts raw collected JSON tweets to CSV for further processing

Usage:
    Run 'python3 raw_json_to_csv.py --help' for instructions

"""

import argparse
import json
import glob
import pandas as pd
import os
import re


def argparse_init():
    """Prepare ArgumentParser for inputs."""

    p = argparse.ArgumentParser(
            description='Convert ingested tweet JSON files to single CSV',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('input_pattern',
                   help='Pattern for glob to find all json files.',
                   type=str)
    p.add_argument('output_csv',
                   help='Path for output csv. Will fail if it already exists.',
                   type=str)

    return p


def hashtag_text(entity):
    """Retrieve list of hashtags from tweets "entity" object"""
    hash_list = entity['hashtags']

    if len(hash_list) == 0:
        return None
    else:
        hash_text_list = []
        for ht in hash_list:
            hash_text_list.append('#{}'.format(ht['text']))

        return ', '.join(hash_text_list)


def extract_urls(entity):
    """Retrieve list of URLs from tweets "entity" object"""

    url_list = entity['urls']

    if len(url_list) == 0:
        return None
    else:
        url_link_list = []
        for url in url_list:
            url_link_list.append(url['expanded_url'])

        return ', '.join(url_link_list)


def extract_user_mentions(entity):
    """Retrieve list of user mentions from tweets "entity" object"""

    um_list = entity['user_mentions']

    if len(um_list) == 0:
        return None
    else:
        um_sn_list = []
        for um in um_list:
            um_sn_list.append('@{}'.format(um['screen_name']))

        return ', '.join(um_sn_list)


def extract_user_atts(df):
    """Retrieve user attributes for all authors in tweet dataframe"""

    user_id_list = [u['id'] for u in df['user']]
    screen_name_list = [u['screen_name'] for u in df['user']]
    joined_date_list = [u['created_at'] for u in df['user']]

    return screen_name_list, user_id_list, joined_date_list


def remove_format_destroyers(string):
    string = re.sub('\n', '{[NEWLINE]}', string)
    string = re.sub('\r', '{[RETURN]}', string)
    string = re.sub('\t', '{[TAB]}', string)
    # The above also handles the case of '\r\n'
    return(string)


def process_single_json(path):
    """Read and process a single JSON line tweet file, outputting clean dataframe"""
    input_df = pd.read_json(path, lines=True)
    output_df = pd.DataFrame(None, index=input_df.index, columns=[])

    output_df['Tweet Id'] = input_df['id_str']
    output_df['Timestamp'] =  ['{} UTC'.format(ts) for ts in input_df['created_at']]
    output_df['Text'] = [remove_format_destroyers(t) for t in input_df['text']]
    output_df['Hashtags'] = [hashtag_text(ent) for ent in input_df['entities']]
    output_df['URLS'] = [extract_urls(ent) for ent in input_df['entities']]
    output_df['User Mentions'] = [extract_user_mentions(ent) for ent in input_df['entities']]
    output_df['In Reply To Tweet ID'] = input_df['in_reply_to_status_id']
    output_df['In Reply to User'] = input_df['in_reply_to_user_id']
    output_df['Retweet_Count'] = input_df['retweet_count']
    output_df['Screen Name'], output_df['User Id'], output_df['Joined Twitter'] = extract_user_atts(input_df)
    output_df['Coordinates'] = [str([c]) for c in input_df['coordinates']]
    output_df.loc[output_df['Coordinates'] == '[nan]', 'Coordinates'] = '[]'
    output_df['Original Tweet'] = ['http://twitter.com/{}/statuses/{}'.format(t['Screen Name'], t['Tweet Id'])
                                   for i, t in output_df.iterrows()]

    return output_df


def main():

    parser = argparse_init()
    args = parser.parse_args()

    if os.path.isfile(args.output_csv):
        raise ValueError('Output CSV already exists. Delete and try again.')

    json_list = glob.glob(args.input_pattern)

    output_list = [process_single_json(path) for path in json_list]
    output_df = pd.concat(output_list)

    output_df.to_csv(args.output_csv, index=False)


if __name__ == '__main__':
    main()
