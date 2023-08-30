import os
import sys
import logging
import math as math
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd

# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utilities.db import *


# warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
# mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-tanda')
# interswitch_uganda_hook = MySqlHook(mysql_conn_id='interswitch_uganda_server', schema='iswug_staging')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")


def process_dates(df):
    df['time_'] = pd.to_datetime(df['time_'], format='%Y-%m-%d %H:%M')

    df['transaction_dates'] = [x.strftime('%d-%m-%Y') for x in df['time_']]

    df['transaction_dates'] = pd.to_datetime(df['transaction_dates'], format='%d-%m-%Y')

    df['year_month_transaction_dates'] = [x.strftime('%b-%Y') for x in df['time_']]

    return df


def calculate_commissions_summaries(df):
    max_transactions_date = pd.to_datetime('today')

    # max_transactions_date = df['transaction_dates'].max()

    df_last_6_months = df[df['transaction_dates'] > max_transactions_date + relativedelta(months=-6)]

    commissions = df_last_6_months[
        (df_last_6_months['biller'] == 'Agent Commission') & (df_last_6_months['status'] == 'Approved')]

    commissions_summaries = commissions.groupby('terminal').agg(
        total_commissions_amount=pd.NamedAgg('credit_amt', aggfunc='sum'),
        unique_number_of_commissions=pd.NamedAgg('transaction_dates', aggfunc='nunique'),
        unique_number_of_services_offered=pd.NamedAgg('narration', aggfunc='nunique'),
        number_of_months_received_commissions=pd.NamedAgg('year_month_transaction_dates',
                                                          aggfunc='nunique')).reset_index()

    return commissions_summaries


def calculate_6_months_scoring_summaries(df):
    max_transactions_date = pd.to_datetime('today')
    # max_transactions_date = df['transaction_dates'].max()

    df_last_6_months = df[df['transaction_dates'] >= max_transactions_date + relativedelta(months=-6)]

    debits_df_last_6_months = df_last_6_months[
        (df_last_6_months['debit_amt'] > 0) & (df_last_6_months['status'] == 'Approved')]

    debits_df_last_6_months_negative_balances = debits_df_last_6_months[debits_df_last_6_months['balance'] < 0]

    negative_balances_summaries_last_6_months = debits_df_last_6_months_negative_balances.groupby('terminal').agg(
        lowest_negative_balance=pd.NamedAgg('balance', min),
        highest_negative_balance=pd.NamedAgg('balance', max),
        earliest_negative_balance_date=pd.NamedAgg('transaction_dates', min),
        latest_negative_balance_date=pd.NamedAgg('transaction_dates', max),
        unique_negative_balance_dates=pd.NamedAgg('transaction_dates', 'nunique')).reset_index()

    debits_df_last_6_months_without_negative_balances = debits_df_last_6_months[debits_df_last_6_months['balance'] >= 0]

    agent_summaries_last_6_months = debits_df_last_6_months_without_negative_balances.groupby('terminal').agg(
        total_debit_amount=pd.NamedAgg('debit_amt', sum),
        average_transaction_size=pd.NamedAgg('debit_amt', 'mean'),
        total_transactions=pd.NamedAgg('request_ref', 'nunique'),
        earliest_transaction_date=pd.NamedAgg('transaction_dates', min),
        latest_transaction_date=pd.NamedAgg('transaction_dates', max),
        no_of_services_offered=pd.NamedAgg('biller', 'nunique'),
        min_balance=pd.NamedAgg('balance', min),
        max_balance=pd.NamedAgg('balance', max),
        average_balance=pd.NamedAgg('balance', 'mean'),
        min_debit_amt=pd.NamedAgg('debit_amt', min),
        max_debit_amt=pd.NamedAgg('debit_amt', max),
        unique_transaction_days=pd.NamedAgg('transaction_dates', 'nunique'),
        unique_transaction_months=pd.NamedAgg('year_month_transaction_dates', 'nunique')).reset_index()

    agent_summaries_last_6_months['expected_transaction_days_last_6_months'] = (
                (max_transactions_date - (max_transactions_date + relativedelta(months=-6))) / np.timedelta64(1,
                                                                                                              'D') + 1)

    agent_summaries_last_6_months['daily_trading_consistency_last_6_months'] = round(
        agent_summaries_last_6_months['unique_transaction_days'] / agent_summaries_last_6_months[
            'expected_transaction_days_last_6_months'], 2)

    agent_summaries_last_6_months['average_daily_transactions'] = round(
        agent_summaries_last_6_months['total_transactions'] / agent_summaries_last_6_months['unique_transaction_days'])

    agent_summaries_last_6_months['average_daily_debit_amt'] = round(
        agent_summaries_last_6_months['total_debit_amount'] / agent_summaries_last_6_months[
            'expected_transaction_days_last_6_months'])

    agent_summaries_last_6_months['days_since_last_transaction'] = (
                (max_transactions_date - agent_summaries_last_6_months['latest_transaction_date']) / np.timedelta64(1,
                                                                                                                    'D')).astype(
        int)

    agent_summaries_last_6_months = pd.merge(agent_summaries_last_6_months, negative_balances_summaries_last_6_months,
                                             on='terminal', how='left')

    agent_summaries_last_6_months['lowest_negative_balance'].fillna(0, inplace=True)

    agent_summaries_last_6_months['highest_negative_balance'].fillna(0, inplace=True)

    agent_summaries_last_6_months['unique_negative_balance_dates'].fillna(0, inplace=True)

    agent_summaries_last_6_months['earliest_negative_balance_date'].fillna(
        agent_summaries_last_6_months['latest_transaction_date'], inplace=True)

    agent_summaries_last_6_months['latest_negative_balance_date'].fillna(
        agent_summaries_last_6_months['latest_transaction_date'], inplace=True)

    agent_summaries_last_6_months['days_since_latest_negative_balance'] = ((agent_summaries_last_6_months[
                                                                                'latest_transaction_date'] -
                                                                            agent_summaries_last_6_months[
                                                                                'latest_negative_balance_date']) / np.timedelta64(
        1, 'D')).astype(int)

    return agent_summaries_last_6_months


def calc_trading_consistency_score(df):
    trading_consistency = df['daily_trading_consistency_last_6_months']

    if trading_consistency >= 0 and trading_consistency < 0.30:
        return -100
    elif trading_consistency >= 0.30 and trading_consistency < 0.50:
        return 0
    elif trading_consistency >= 0.50 and trading_consistency < 0.60:
        return 50
    elif trading_consistency >= 0.60 and trading_consistency < 0.75:
        return 100
    elif trading_consistency >= 0.75 and trading_consistency < 0.90:
        return 150
    elif trading_consistency >= 0.90 and trading_consistency <= 1.00:
        return 200


def age_on_network_score(df):
    age = df['evaluation_months']

    if age >= 0 and age <= 1:
        return -100
    if age > 1 and age <= 3:
        return 0
    elif age > 3 and age <= 4:
        return 50
    elif age > 4 and age <= 5:
        return 100
    elif age > 5:
        return 200


def recency_in_months_score(df):
    recency = df['diff_last_txn_month']

    if recency >= 0 and recency < 1:
        return 200
    elif recency >= 1 and recency < 2:
        return 150
    elif recency >= 2 and recency < 4:
        return 100
    elif recency >= 4 and recency < 6:
        return 0
    elif recency >= 6 and recency < 8:
        return -50
    elif recency >= 8 and recency < 10:
        return -100
    elif recency >= 10:
        return -200


# defining a function to score for average monthly daily_debit
def average_daily_debit_score(df):
    daily_debit = df['average_daily_debit_amt']

    if daily_debit >= 0 and daily_debit < 20000:
         return -100
    elif daily_debit >= 20000 and daily_debit < 35000:
        return 0
    elif daily_debit >= 35000 and daily_debit < 50000:
        return 25
    elif daily_debit >= 50000 and daily_debit < 100000:
        return 35
    elif daily_debit >= 100000 and daily_debit < 200000:
        return 50
    elif daily_debit >= 200000 and daily_debit < 500000:
        return 75
    elif daily_debit >= 500000 and daily_debit < 750000:
        return 100
    elif daily_debit >= 750000 and daily_debit < 1000000:
        return 150
    elif daily_debit >= 1000000:
        return 200


def unique_number_of_commissions_score(df):
    commissions = df['unique_number_of_commissions']

    if commissions >= 0 and commissions < 1:
        return -150
    elif commissions >= 1 and commissions < 2:
        return -100
    elif commissions >= 2 and commissions < 3:
        return 0
    elif commissions >= 3 and commissions < 5:
        return 100
    elif commissions >= 5 and commissions < 6:
        return 150
    elif commissions >= 6:
        return 200


def load_staging_db_data(terminal_id: str):
    df = interswitch_uganda_hook.get_pandas_df(
        sql="""
            SELECT TerminalId as terminal, MifosClientId as client_id FROM Customers where TerminalId = %(terminal_id)s 
        """,
        parameters={'terminal_id': terminal_id}
    )
    return df


def load_loans_data(mifos_client_id: int, mifos_product_id: int) -> pd.DataFrame:

    df = mifos_hook.get_pandas_df(
        sql="""
            SELECT 
                id, client_id, principal_disbursed_derived, disbursedon_date, expected_maturedon_date,
                total_expected_repayment_derived
            FROM `mifostenant-uganda`.m_loan 
            where product_id = %(product_id)s and disbursedon_date is not null 
            and loan_status_id in (300,600,700) and client_id = %(mifos_client_id)s
        """,
        parameters={
            'mifos_client_id': mifos_client_id,
            'product_id': mifos_product_id
        }
    )

    return df


def load_transactions_data(client_id):
    df = mifos_hook.get_pandas_df(
        sql="""
            SELECT 
                mlt.loan_id, mlt.transaction_date, mlt.amount 
            from `mifostenant-uganda`.m_loan ml left join
            `mifostenant-uganda`.m_loan_transaction mlt on ml.id = mlt.loan_id  
            where mlt.transaction_type_enum = 2 and mlt.is_reversed = 0
            and ml.client_id = %(client_id)s
        """,
        parameters={'client_id': client_id}
    )
    return df


def calculate_loan_count_bands(df):
    loan_count = df['count_of_loans']

    if loan_count == 0:
        return 'Band 1'
    elif loan_count >= 1 and loan_count <= 2:
        return 'Band 2'
    elif loan_count >= 3 and loan_count <= 5:
        return 'Band 3'
    elif loan_count >= 6 and loan_count <= 9:
        return 'Band 4'
    elif loan_count >= 10:
        return 'Band 5'


def calculate_repayments_bands(df):
    repayments = df['%repayment_by_dpd_7']

    if repayments >= 0 and repayments <= 29:
        return 'Band 1'
    elif repayments >= 30 and repayments <= 49:
        return 'Band 2'
    elif repayments >= 50 and repayments <= 69:
        return 'Band 3'
    elif repayments >= 70 and repayments <= 89:
        return 'Band 4'
    elif repayments >= 90 and repayments <= 99:
        return 'Band 5'
    elif repayments >= 100:
        return 'Band 6'


def calculate_limit_factor(df):
    """
    Input - loan band and repayment band

    Function - Calculate limit factor based on inputs (limit matrix)

    Output - Limit factor based on inputs

    """

    loan_band = df['loan_band']
    repayment_band = df['repayment_band']

    if loan_band == 'Band 1':
        return 0.119
    elif loan_band == 'Band 2' and repayment_band == 'Band 4':
        return 0.083
    elif loan_band == 'Band 2' and repayment_band == 'Band 5':
        return 0.108
    elif loan_band == 'Band 2' and repayment_band == 'Band 6':
        return 0.128
    elif loan_band == 'Band 3' and repayment_band == 'Band 4':
        return 0.098
    elif loan_band == 'Band 3' and repayment_band == 'Band 5':
        return 0.123
    elif loan_band == 'Band 3' and repayment_band == 'Band 6':
        return 0.136
    elif loan_band == 'Band 4' and repayment_band == 'Band 4':
        return 0.113
    elif loan_band == 'Band 4' and repayment_band == 'Band 5':
        return 0.138
    elif loan_band == 'Band 4' and repayment_band == 'Band 6':
        return 0.145
    elif loan_band == 'Band 5' and repayment_band == 'Band 4':
        return 0.128
    elif loan_band == 'Band 5' and repayment_band == 'Band 5':
        return 0.153
    elif loan_band == 'Band 5' and repayment_band == 'Band 6':
        return 0.17
    else:
        return 0.0


def round_off(n):
    """
    This function rounds off elements by setting a ceiling to the next 100
    """
    return int(math.ceil(n / 100.0)) * 100


def amounts_cap(n):
    if n < 35000:
        return 0
    elif n > 1000000:
        return 1000000
    else:
        return n


def determine_limit_for_first_loan(df):
    """""
    Input - Dataframe with loan count and allocated limit

    Function - If loan count is 0, and allocated limit is greater than 0,
    terminal is allocated 50% of original limit and if this 50% is below product minimum, return product minimum

    Output - Final limit for a first time loan taker.

    """""
    loan_count = df['count_of_loans']
    limit = df['final_3_day_limit']

    if loan_count == 0 and limit > 0:
        limit = (.5 * limit)
        if limit < 35000:
            return 35000
        else:
            return limit
    else:
        return limit


def get_scoring_refresh_date():
    scoring_refresh_date = (pd.Timestamp.today()).strftime("%Y-%m-%d")
    scoring_referesh_date = pd.Timestamp(scoring_refresh_date)

    return scoring_refresh_date


def get_model_version() -> str:
    """
    function to add date when scoring refresh was done

    Inputs:
    Model refresh date

    Outputs:
    new column with scoring refresh date

    """

    model_version = f"2023-001[2023-03-22, {pd.to_datetime('today').date()}]"

    return model_version


def calculate_scores(data: pd.DataFrame, loans_data_staging: pd.DataFrame, transactions_data_df: pd.DataFrame):

    # diff in months duration
    current_period = data["latest_transaction_date"].max()
    data['latest_trading_month'] = current_period

    data['evaluation_months'] = (
            (data['latest_trading_month'] - data['earliest_transaction_date']) / np.timedelta64(1, 'M') + 1)
    data['evaluation_months'] = data['evaluation_months'].apply(lambda x: np.ceil(x))

    # get time period between current period and last transaction
    data["diff_last_txn_month"] = round(((current_period - data["latest_transaction_date"]) / np.timedelta64(1, 'M')),
                                        0)

    # #### Combining all scores

    # adding the scores columns to the dataframe by applying the functions to the dataframe
    data['trading_consistency_score'] = data.apply(lambda x: calc_trading_consistency_score(x), axis=1)
    data['age_on_network_score'] = data.apply(lambda x: age_on_network_score(x), axis=1)
    data['recency_in_months_score'] = data.apply(lambda x: recency_in_months_score(x), axis=1)
    data['average_daily_debit_score'] = data.apply(lambda x: average_daily_debit_score(x), axis=1)
    data['unique_number_of_commissions_score'] = data.apply(lambda x: unique_number_of_commissions_score(x), axis=1)

    # Sum to get the total score

    data['total_score'] = data.loc[:, ['trading_consistency_score', 'age_on_network_score', 'recency_in_months_score',
                                       'average_daily_debit_score', 'unique_number_of_commissions_score']].sum(axis=1)

    ## merge loans, staging and scoring results
    loans_and_staging_and_results = pd.merge(loans_data_staging, data, on='terminal', how='outer')


    ## get the number of loans per terminal
    loan_counts = loans_and_staging_and_results.groupby('terminal')['id'].nunique().rename(
        'count_of_loans').reset_index()

    ## merge loans per terminal with other df with other info

    loans_and_staging_and_results = pd.merge(loans_and_staging_and_results, loan_counts, on='terminal', how='outer')

    # #### 2. Time in between graduations
    loans_and_staging_and_results[loans_and_staging_and_results['terminal'] == '3IS00106'].sort_values(
        by='disbursedon_date')

    ## rank records so as to be able to pick only the latest loan

    loans_and_staging_and_results['loans_rank'] = loans_and_staging_and_results.groupby('terminal')[
        'disbursedon_date'].rank(ascending=True)

    loans_and_staging_and_results[loans_and_staging_and_results['terminal'] == '3IS00106'].sort_values(
        by='disbursedon_date')

    ## fill null loans_rank with 0
    loans_and_staging_and_results['loans_rank'].fillna(0, inplace=True)

    latest_loan = loans_and_staging_and_results.loc[
        loans_and_staging_and_results.groupby('terminal')['loans_rank'].idxmax()].reset_index()

    ## convert dates from object to datetime

    latest_loan['disbursedon_date'] = pd.to_datetime(latest_loan['disbursedon_date'])

    latest_loan['expected_maturedon_date'] = pd.to_datetime(latest_loan['expected_maturedon_date'])

    ## get difference in months between latest disbursement date and scoring day (today)
    latest_loan['months_since_last_disbursement'] = (pd.to_datetime('today') - latest_loan[
        'disbursedon_date']) / np.timedelta64(1, 'M')

    latest_loan = latest_loan[['terminal', 'months_since_last_disbursement']]

    loans_and_staging_and_results = pd.merge(loans_and_staging_and_results, latest_loan, on='terminal')

    ## calculate end rollover date and dpd 7 date to allow for calculating repayments by dpd7
    loans_and_staging_and_results['end_rollover_date'] = loans_and_staging_and_results[
                                                             'expected_maturedon_date'] + pd.Timedelta(days=2)

    loans_and_staging_and_results['dpd_7_date'] = loans_and_staging_and_results['end_rollover_date'] + pd.Timedelta(
        days=7)

    ## rename column to allow for merging with transactions data
    loans_and_staging_and_results.rename(columns={'id': 'loan_id'}, inplace=True)

    ## merge loans and repayments with a left join as repayments data was not filtered for this product only
    repayments_df = pd.merge(loans_and_staging_and_results, transactions_data_df, on='loan_id', how='left')

    ## calculate sum of repayments by dpd 7 per client as hurdle rate is average not on a per loan basis
    repayments_by_dpd_7 = repayments_df[repayments_df['transaction_date'] < repayments_df['dpd_7_date']].groupby(
        'client_id').agg(repayments_by_dpd_7=pd.NamedAgg('amount', 'sum')).reset_index()

    ## calculate total principal disbursed on a per client basis
    total_prin = loans_data_staging.groupby('client_id')['principal_disbursed_derived'].sum().rename(
        'total_principal').reset_index()

    ## merge repayments by dpd 7 and total principal to allow for calculation of the hurdle rate
    hurdle_rates = pd.merge(repayments_by_dpd_7, total_prin, on='client_id', how='outer')

    ## calculate actual hurdle rate and express as %
    hurdle_rates['%repayment_by_dpd_7'] = round(
        hurdle_rates['repayments_by_dpd_7'] / hurdle_rates['total_principal']) * 100

    ## merge hurdle rates with other df with other info
    final_df = pd.merge(loans_and_staging_and_results, hurdle_rates, on='client_id', how='outer')

    final_df['loan_band'] = final_df.apply(lambda x: calculate_loan_count_bands(x), axis=1)

    final_df['repayment_band'] = final_df.apply(lambda x: calculate_repayments_bands(x), axis=1)

    final_df['limit_factor'] = final_df.apply(lambda x: calculate_limit_factor(x), axis=1)

    # #### Limit Allocation
    final_df['minimum_limit'] = final_df['average_daily_debit_amt'] * 30 * final_df['limit_factor']

    final_df['rounded_limit'] = final_df['minimum_limit'].apply(round_off)

    final_df['final_3_day_limit'] = final_df['rounded_limit'].apply(amounts_cap)

    final_df['final_3_day_limit'] = final_df.apply(lambda x: determine_limit_for_first_loan(x), axis=1)

    final_df["scoring_refresh_date"] = get_scoring_refresh_date()

    final_df["model_version"] = get_model_version()

    return final_df


def get_prev_scoring_results(terminal:str):
    df = warehouse_hook.get_pandas_df(
        sql="""
            SELECT terminal, final_3_day_limit as previous_limit FROM interswitch_ug.scoring_results_view where terminal = %(terminal)s
         """,
        parameters={'terminal': terminal}
    )
    if df.empty:
        df = pd.DataFrame({
            'terminal': [terminal],
            'previous_limit': [0]
        }, index=[0])

    return df


def determine_if_to_graduate(df):
    months_since_last_disbursement = df['months_since_last_disbursement']
    loan_count = df['count_of_loans']
    current_limit = df['current_limit']
    previous_limit = df['previous_limit']

    if months_since_last_disbursement <= 1 and loan_count > 1 and previous_limit == 0:
        return current_limit
    elif months_since_last_disbursement <= 1 and loan_count > 1 and previous_limit > 0:
        return previous_limit
    else:
        return current_limit


def determine_if_qualified(df):
    df['is_qualified'] = False

    df.loc[(df['final_3_day_limit'] > 0) & (df['total_score'] >= 600), 'is_qualified'] = True

    return df


def rules_summary_narration(df):
    trading_consistency_score = df['trading_consistency_score']
    age_on_network_score_ = df['age_on_network_score']
    recency_in_months_score_ = df['recency_in_months_score']
    average_daily_debit_score_ = df['average_daily_debit_score']
    unique_number_of_commissions_score_ = df['unique_number_of_commissions_score']
    total_score = df['total_score']
    limit_factor = df['limit_factor']
    ## not found in table
    final_allocated_limit_3_day = df['final_3_day_limit']
    is_qualified = df['is_qualified']


    if trading_consistency_score <= 0 and is_qualified == False:
        return 'Trading consistency is below set threshold and total score is < 600. : Based on historical and current information on your business and loan history, you do not currently meet Asante lending criteria: C004'
    elif average_daily_debit_score_ <= 0 and is_qualified == False:
        return 'Average daily debit amount is below set threshold and total score is < 600. : Based on historical and current information on your business and loan history, you do not currently meet Asante lending criteria: C004'
    elif total_score < 600 and is_qualified == False:
        return 'Total score is below 600. : Based on historical and current information on your business and loan history, you do not currently meet Asante lending criteria: C004'
    elif age_on_network_score_ <= 0 and is_qualified == False:
        return 'Age on network is below set threshold and total score is < 600. : Based on historical and current information on your business and loan history, you do not currently meet Asante lending criteria: C004'
    elif recency_in_months_score_ <= 0 and is_qualified == False:
        return 'Number of months with no activity is below set threshold and total score is < 600. : Based on historical and current information on your business and loan history, you do not currently meet Asante lending criteria: C004'
    elif unique_number_of_commissions_score_ <= 0 and is_qualified == False:
        return 'Number of commission payments is below set threshold and total score is < 600. : Based on historical and current information on your business and loan history, you do not currently meet Asante lending criteria: C004'
    elif limit_factor == 0.00:
        return 'Inconsistent Repayments. : You have been inconsistent in paying your loans on time - please consistently pay your loans on time: A001'
    elif final_allocated_limit_3_day > 0 and is_qualified == True:
        return 'All rules passed. : Limits assigned per lending criteria : F001'
    else:
        return 'Limit below product min : Limits assigned less than product thresholds: D001'


def get_scoring_results(config_path, raw_data) -> str or None:
    product_id = 2
    raw_data = process_dates(raw_data)

    commissions_summaries = calculate_commissions_summaries(raw_data)
    agent_summaries_last_6_months = calculate_6_months_scoring_summaries(raw_data)
    summaries_data = pd.merge(commissions_summaries, agent_summaries_last_6_months, on='terminal')

    client_data = load_staging_db_data(terminal_id=raw_data.iloc[0]['terminal'])

    failure_reason = None
    if commissions_summaries.shape[0] == 0:
        failure_reason = 'Client does not have commission payments'
        logging.warning('Client does not have commission payments')
    elif client_data.shape[0] == 0:
        failure_reason = 'Client not Found in Customers Table'
        logging.warning('Client not Found in Customers Table')
    else:
        loans_data = load_loans_data(
            mifos_client_id=client_data.iloc[0]['client_id'],
            mifos_product_id=product_id
        )

        loans_data_staging_ = pd.merge(
            client_data,
            loans_data,
            on='client_id',
            how='inner'
        )
        transactions_data = load_transactions_data(client_id=client_data.iloc[0]['client_id'])

        results = calculate_scores(
            data=summaries_data,
            loans_data_staging=loans_data_staging_,
            transactions_data_df=transactions_data
        )

        previous_results = get_prev_scoring_results(terminal=client_data.iloc[0]['terminal'])

        current_results = results[['terminal', 'final_3_day_limit']]

        current_results.rename(columns={'final_3_day_limit': 'current_limit'}, inplace=True)

        scoring_results = pd.merge(previous_results, current_results, on = 'terminal')

        scoring_results['previous_limit'].fillna(0, inplace=True)

        results = pd.merge(results, scoring_results, on='terminal')

        results['final_3_day_limit'] = results.apply(
            lambda x: determine_if_to_graduate(x),
            axis=1
        )

        results = determine_if_qualified(results)

        results['rules_summary_narration'] = results.apply(lambda x: rules_summary_narration(x), axis=1)

        results[['rules_summary_narration', "communication_to_client", "limit_reason_code"]] = results[
            "rules_summary_narration"].astype("str").str.split(":", expand=True)

        results['tenure'] = 3

        results = results.head(n=1)

        results.rename(columns={'%repayment_by_dpd_7': 'percentage_repayment_by_dpd_7'}, inplace=True)

        warehouse_hook.insert_rows(
            table='interswitch_ug.scoring_results',
            target_fields=[
                'terminal', 'total_debit_amount', 'average_transaction_size', 'total_transactions', 'earliest_transaction_date',
                'latest_transaction_date', 'no_of_services_offered', 'min_balance', 'max_balance', 'average_balance',
                'min_debit_amt', 'max_debit_amt', 'unique_transaction_days', 'unique_transaction_months',
                'expected_transaction_days_last_6_months', 'daily_trading_consistency_last_6_months',
                'average_daily_transactions', 'average_daily_debit_amt', 'days_since_last_transaction', 'lowest_negative_balance',
                'highest_negative_balance', 'total_commissions_amount',
                'unique_number_of_commissions', 'unique_number_of_services_offered', 'number_of_months_received_commissions',
                'scoring_refresh_date', 'model_version', 'minimum_limit', 'rounded_limit', 'is_qualified',
                'earliest_negative_balance_date', 'latest_negative_balance_date', 'unique_negative_balance_dates',
                'days_since_latest_negative_balance','latest_trading_month', 'evaluation_months', 'diff_last_txn_month',
                'trading_consistency_score', 'age_on_network_score', 'recency_in_months_score',
                'average_daily_debit_score', 'unique_number_of_commissions_score', 'total_score',
                'count_of_loans', 'months_since_last_disbursement', 'percentage_repayment_by_dpd_7',
                'loan_band', 'repayment_band', 'limit_factor', 'final_3_day_limit', 'previous_limit', 'tenure',
                'rules_summary_narration', 'communication_to_client', 'limit_reason_code'
            ],
            replace=False,
            rows=tuple(results[[
                'terminal', 'total_debit_amount', 'average_transaction_size', 'total_transactions', 'earliest_transaction_date',
                'latest_transaction_date', 'no_of_services_offered', 'min_balance', 'max_balance', 'average_balance',
                'min_debit_amt', 'max_debit_amt', 'unique_transaction_days', 'unique_transaction_months',
                'expected_transaction_days_last_6_months', 'daily_trading_consistency_last_6_months',
                'average_daily_transactions', 'average_daily_debit_amt', 'days_since_last_transaction', 'lowest_negative_balance',
                'highest_negative_balance', 'total_commissions_amount',
                'unique_number_of_commissions', 'unique_number_of_services_offered', 'number_of_months_received_commissions',
                'scoring_refresh_date', 'model_version', 'minimum_limit', 'rounded_limit', 'is_qualified',
                'earliest_negative_balance_date', 'latest_negative_balance_date', 'unique_negative_balance_dates',
                'days_since_latest_negative_balance','latest_trading_month', 'evaluation_months', 'diff_last_txn_month',
                'trading_consistency_score', 'age_on_network_score', 'recency_in_months_score',
                'average_daily_debit_score', 'unique_number_of_commissions_score', 'total_score',
                'count_of_loans', 'months_since_last_disbursement', 'percentage_repayment_by_dpd_7',
                'loan_band', 'repayment_band', 'limit_factor', 'final_3_day_limit', 'previous_limit', 'tenure',
                'rules_summary_narration', 'communication_to_client', 'limit_reason_code'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=1
        )

    return failure_reason


def combine_files(config_path, agent_id):
        files = get_objects(
            aws_access_key_id=Variable.get('AFSG_aws_access_key_id') if Variable.get('DEBUG') == 'FALSE' else None,
            aws_secret_access_key=Variable.get('AFSG_aws_secret_access_key')if Variable.get('DEBUG') == 'FALSE' else None,
            search=agent_id,
            bucket_name=Variable.get('ISWUG_aws_s3_bucket_name')
        )

        if len(files) > 0:
            combined = pd.concat([x['data'] for x in files], ignore_index=True)
            combined['Balance'] = combined['Balance'].apply(lambda x: float(str(x).replace(',', '')) if not pd.isnull(x) else x)
            combined['CreditAmount'] = combined['CreditAmount'].apply(
                lambda x: float(str(x).replace(',', '')) if not pd.isnull(x) else x)
            combined['DebitAmount'] = combined['DebitAmount'].apply(
                lambda x: float(str(x).replace(',', '')) if not pd.isnull(x) else x)

            combined.rename(columns={
                'Date': 'time_', 'Terminal': 'terminal', 'RequestRef': 'request_ref',
                'TranDesc': 'trxn_type', 'Biller': 'biller', 'Narration': 'narration',
                'DebitAmount': 'debit_amt', 'CreditAmount': 'credit_amt', 'Balance': 'balance', 'Status': 'status'
            }, inplace=True)


            combined.drop_duplicates(subset=combined.columns.tolist(), inplace=True)

            if combined.shape[0] > 0:
                csv_buffer = StringIO()
                combined.to_csv(path_or_buf=csv_buffer, index=False)
                save_file_to_s3(
                    s3_file_key=f"interswitch_uganda/scoring_data/{agent_id}_cleaned.csv",
                    bucket_name='afsg-ds-prod-postgresql-dwh-archive',
                    file_bytes=csv_buffer.getvalue()
                )
                return combined
            else:
                logging.warning(f'All files for {agent_id} are empty')

        return None


def trigger_scoring(config_path, agent_id):
    if agent_id is not None:
        raw_data = combine_files(config_path, agent_id)
        if raw_data is not None:
            failure_reason = get_scoring_results(config_path, raw_data=raw_data)
            return True
        else:
            raise pd.errors.EmptyDataError

    return False


if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    args.add_argument("--agent_id", default="3IS02066")
    parsed_args = args.parse_args()

    print(f'\nScoring {parsed_args.agent_id} ...')
    print(trigger_scoring(parsed_args.config, agent_id=parsed_args.agent_id))
    print('=============================================================================\n')