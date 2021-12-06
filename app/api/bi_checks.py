"""Registers the necessary routes for the basic API endpoints."""

import flask
from flask import request, Response
from flask_restful import inputs

from datetime import datetime
import gspread
from io import StringIO
from oauth2client.service_account import ServiceAccountCredentials

import numpy as np
import pandas as pd

from app.api import api


""" Script that compares new BI data to existing snapshot in this repo and runs some QA checks.

Steps for use:
1) Confirm that the local repo contains the latest US breakthrough infection data snapshot.
2) Download a CSV of the current Google Sheet snapshot and save it as "new.csv" to the top-level
   directory.
3) Run this script from the top-level directory:
   python scripts/check-new-bi-data.py

The output is printed out in a comma-separated string, meant for pasting directly into a Google
Sheet (and letting it algorithmically separate into columns).
"""


###########################################################################################
############        Load data and convert to numeric if needed         ####################
###########################################################################################


_EXISTING_CSV_URL = 'https://raw.githubusercontent.com/pandemic-tracking/bi/main/US%20states%20breakthrough%20reporting%20-%20Snapshot.csv'


def make_output_string(gc):

    out = ''

    # Load new CSV file (current spreadsheet)
    sh = gc.open_by_key(flask.current_app.config['SNAPSHOT_SHEET_ID'])
    worksheet = sh.worksheet("Snapshot")
    # need to get everything as strings first and let pandas do the conversion later
    records = [{k: str(v) for k, v in record.items()} for record in worksheet.get_all_records()]
    new_df = pd.DataFrame(records)

    # Load existing sheet snapshot from GitHub
    existing_df = pd.read_csv(_EXISTING_CSV_URL)

    first_numeric_colname = 'BI cases'
    last_numeric_colname = 'Total Individuals not fully vaccinated'
    first_numeric_col = list(new_df.columns).index(first_numeric_colname)
    last_numeric_col = list(new_df.columns).index(last_numeric_colname)

    possible_numeric_cols = list(new_df.columns[first_numeric_col:last_numeric_col+1])

    # some of these are percent columns and should be read as such
    percent_cols = [x for x in possible_numeric_cols if 'percent' in x]
    numeric_cols = [x for x in possible_numeric_cols if 'percent' not in x]

    # do numeric and percent conversions
    for col in numeric_cols:
        for df in [new_df, existing_df]:
            # if the column isn't already a float, replace strings and make numeric
            if col not in dict(df.dtypes):
                continue
            if dict(df.dtypes)[col] == object:
                df[col] = df[col].str.replace(',', '')
                # also replace "X" characters with the number 1: they're generally placeholders
                df[col] = df[col].str.replace('X', '1')
                df[col] = pd.to_numeric(df[col])  # convert to numeric

    def p2f(x):
        try:
            if pd.isnull(x):
                return np.nan
            else:
                return float(x.strip('%'))/100
        except Exception:
            return np.nan

    for col in percent_cols:
        for df in [new_df, existing_df]:
            if col not in dict(df.dtypes):
                continue
            if dict(df.dtypes)[col] == object:
                df[col] = df[col].str.replace('X', '1')
            df[col] = df[col].apply(p2f)


    ###########################################################################################
    ##################           Run a series of QA checks         ############################
    ###########################################################################################

    # Check any states that were either dropped or added
    new_states = set(new_df.Abbr)
    old_states = set(existing_df.Abbr)
    states_added = new_states.difference(old_states)
    states_dropped = old_states.difference(new_states)

    out += 'State,Issue,Metric,Details\n'
    for state in states_dropped:
        out += '%s,State removed!\n' % state
    for state in states_added:
        out += '%s,State added\n' % state

    # Check any columns that were either dropped or added
    old_columns = set(existing_df.columns)
    new_columns = set(new_df.columns)

    columns_added = new_columns.difference(old_columns)
    for col in columns_added:
        if col.startswith('Unnamed'):
            continue
        out += 'All,New column added,%s\n' % col

    columns_removed = old_columns.difference(new_columns)
    for col in columns_removed:
        if col.startswith('Unnamed'):
            continue
        out += 'All,Column removed,%s\n' % col

    # Check specific data for each state
    for state in existing_df.Abbr:
        new_row = new_df.loc[new_df.Abbr == state]
        old_row = existing_df.loc[existing_df.Abbr == state]
        state = old_row.Abbr.iloc[0]
        
        for col in numeric_cols:
            if not (col in old_row and col in new_row):
                continue
            old_value = old_row[col].iloc[0]
            new_value = new_row[col].iloc[0]

            # Check things that were previously reported that are not now
            if pd.isnull(new_value) and not pd.isnull(old_value):
                out += '%s,Lost metric,%s,Old value %d\n' % (
                    state, col, old_value)
                
            # Check things that were previously not reported and are reported now
            if not pd.isnull(new_value) and pd.isnull(old_value):
                out += '%s,New metric,%s,New value %d\n' % (
                    state, col, new_value)
                
            if pd.isnull(new_value) or pd.isnull(old_value):
                continue
                
            # Check any cumulative numbers that went down
            if new_value < old_value:
                # whitelist certain columns from triggering alerts
                whitelist_cols = ['Total Individuals not fully vaccinated']
                if col not in whitelist_cols:
                    out += '%s,Cumulative decrease,%s,%d -> %d\n' % (
                        state, col, old_value, new_value)
                    
            # Increase magnitude check, alert if >2x
            if new_value > 2 * old_value:
                out += '%s,>2x increase,%s,%d -> %d\n' % (
                    new_row.Abbr.iloc[0], col, old_value, new_value)
                
        # Do new metric/lost metric checks for percentage columns, but don't compare numbers
        for col in percent_cols:
            if not (col in old_row and col in new_row):
                continue
            old_value = old_row[col].iloc[0]
            new_value = new_row[col].iloc[0]
            
            if pd.isnull(new_value) and not pd.isnull(old_value):
                out += '%s,Lost metric,%s,Old value %.2f\n' % (
                    state, col, old_value)
                
            if not pd.isnull(new_value) and pd.isnull(old_value):
                out += '%s,New metric,%s,New value %.2f\n' % (
                    state, col, new_value)

    return out


@api.route('/bi-checks', methods=['GET'])
def bi_data_check():
    credential = ServiceAccountCredentials.from_json_keyfile_name(
        flask.current_app.config['CREDENTIALS_PATH'],
        [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(credential)

    out = make_output_string(gc)
    # return out
    out_df = pd.read_csv(StringIO(out)).fillna('')

    sh = gc.open_by_key(flask.current_app.config['CHECKS_SHEET_ID'])
    title = datetime.today().strftime('%Y-%m-%d-temp')
    worksheet = sh.add_worksheet(title=title, rows=str(out_df.shape[0]), cols="10")
    worksheet.update([out_df.columns.values.tolist()] + out_df.values.tolist())
    worksheet.format('A1:D1', {'textFormat': {'bold': True}})

    return flask.jsonify('Done: sheet tab %s created' % title)


@api.route('/test', methods=['GET'])
def health_check():
    flask.current_app.logger.info('TEST')
    return flask.jsonify({'all': 'is well'})
