import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import json
import datetime as dt
import math
import pandasql as ps



def format_df(df=None):
    df['timestamp'] = df['timestamp'].apply(lambda x: pd.to_datetime(x, utc=True))
    df['asset'] = df['params'].apply(lambda x: json.loads(x)['a'])
    df['currency'] = df['params'].apply(lambda x: json.loads(x)['c'])
    df['frequency'] = df['params'].apply(lambda x: json.loads(x)['i'])
    df['access'] = df['studio'].apply(lambda x: 'Studio' if x == True else 'API')
    df['user_previous_timestamp' ] = df.groupby('id').timestamp.shift(1)
    df['user_min_timestamp' ] = df.groupby('id').timestamp.transform('min')
    df['hours_since_last_event'] = (df['timestamp'] - df['user_previous_timestamp']) / pd.Timedelta(hours=1)
    df['hours_since_last_event'] = df['hours_since_last_event'].apply(lambda x: math.floor(x) if math.isnan(x) == False else 0)
    df['days_since_last_event'] = (df['timestamp'] - df['user_previous_timestamp']) / pd.Timedelta(days=1)
    df['days_since_last_event'] = df['days_since_last_event'].apply(lambda x: math.floor(x) if math.isnan(x) == False else 0)
    df['days_since_first_event'] = (df['timestamp'] - df['user_min_timestamp']) / pd.Timedelta(days=1)
    df['days_since_first_event'] = df['days_since_first_event'].apply(lambda x: math.floor(x))
    return df



def df_api_access(df=None):
    access = df.groupby('access')['id'].agg(['count','nunique'])
    access = access.rename(columns={'count':'user_logs','nunique':'users'})
    access['logs_per_user'] = round(access['user_logs'] / access['users'],1)
    access = access.drop(columns={'user_logs'})
    return access



def plot_api_access(df=None):
    df = df.reset_index()

    fig, ax1=plt.subplots(figsize=(5,8))
    sns.barplot(x = 'access', y='users', data=df, label='Users', color='grey')

    ax2 = ax1.twinx()
    sns.lineplot(x=ax1.get_xticks(), y='logs_per_user', data=df, color='blue', label='Logs p. User')

    ax1.set_ylim(0,3500)
    ax2.set_ylim(0,400)

    ax1.set_ylabel('Users', fontsize=15)
    ax2.set_ylabel('Logs p. User', fontsize=15)

    ax1.legend(loc = 2, fontsize = 10)
    ax2.legend(loc = 1, fontsize = 10)

    plt.show()
    

    
def df_api_access_split(df=None):
    user_plans = df.groupby('plan')['id'].agg(['count','nunique'])
    user_plans = user_plans.rename(columns={'count':'user_logs','nunique':'users'})
    user_plans['logs_per_user'] = round(user_plans['user_logs'] / user_plans['users'],2)
    return user_plans



def plot_api_access_split(df=None):
    df = df.reset_index()
    fig, ax1=plt.subplots(figsize=(5,8))
    sns.barplot(x = 'plan', y='users', data=df, label='Users', color='grey')

    ax2 = ax1.twinx()
    sns.lineplot(x=ax1.get_xticks(), y='logs_per_user', data=df, color='blue', label='Logs p. User')

    ax1.set_ylim(0,3500)
    ax2.set_ylim(0,400)

    ax1.set_ylabel('Users', fontsize=15)
    ax2.set_ylabel('Logs p. User', fontsize=15)

    ax1.legend(loc = 2, fontsize = 10)
    ax2.legend(loc = 1, fontsize = 10)

    plt.show()
    

def df_api_access_plan_split(df=None):
    df = df.groupby(['access','plan'])['id'].agg(['count','nunique'])
    df = df.rename(columns={'count':'user_logs','nunique':'users'})
    df['logs_per_user'] = round(df['user_logs'] / df['users'],1)
    df = df.drop(columns={'user_logs'})
    return df


def plot_api_access_plan_split(df=None):
    df = df.reset_index()
    df['plan_access'] = df['access'] + '_' + df['plan']


    fig, ax1=plt.subplots(figsize=(5,8))
    sns.barplot(x='plan_access', y='users', data=df, label='Users', color='grey')

    ax2 = ax1.twinx()
    sns.lineplot(x=ax1.get_xticks(), y='logs_per_user', data=df, color='blue', label='Logs p. User')

    ax1.set_ylim(0,3500)
    ax2.set_ylim(0,800)

    ax1.set_ylabel('Users', fontsize=15)
    ax2.set_ylabel('Logs p. User', fontsize=15)

    ax1.legend(loc = 2, fontsize = 10)
    ax2.legend(loc = 1, fontsize = 10)

    plt.show()

    
def get_top_5_metrics(df=None):
    top_metrics = df.copy()
    top_metrics = df.groupby(['plan','metric'])['id'].agg(['count','nunique'])
    top_metrics.reset_index(inplace=True) 
    top_metrics = top_metrics.rename(columns={'count':'total_events','nunique':'users_with_event'})
    top_metrics['events_per_user'] = round(top_metrics['total_events'] / top_metrics['users_with_event'],2)
    top_metrics['rank'] = top_metrics.groupby('plan')['users_with_event'].rank(ascending=False)
    top_metrics = top_metrics[top_metrics['rank'] <= 5].sort_values(['plan','users_with_event'], ascending=[True, False])
    top_metrics.set_index('plan', inplace=True)
    top_metrics[['metric','users_with_event','events_per_user']]
    return top_metrics


def api_df(df=None):
    dfapi = df.copy()
    dfapi['user_previous_timestamp' ] = dfapi.groupby('id').timestamp.shift(1)
    dfapi['user_min_timestamp' ] = dfapi.groupby('id').timestamp.transform('min')
    dfapi['hours_since_last_event'] = (dfapi['timestamp'] - dfapi['user_previous_timestamp']) / pd.Timedelta(hours=1)
    dfapi['hours_since_last_event'] = dfapi['hours_since_last_event'].apply(lambda x: math.floor(x) if math.isnan(x) == False else 0)
    dfapi['days_since_last_event'] = (dfapi['timestamp'] - dfapi['user_previous_timestamp']) / pd.Timedelta(days=1)
    dfapi['days_since_last_event'] = dfapi['days_since_last_event'].apply(lambda x: math.floor(x) if math.isnan(x) == False else 0)
    dfapi['days_since_first_event'] = (dfapi['timestamp'] - dfapi['user_min_timestamp']) / pd.Timedelta(days=1)
    dfapi['days_since_first_event'] = dfapi['days_since_first_event'].apply(lambda x: math.floor(x))
    dfapi['weeks_since_first_event'] = dfapi['days_since_first_event'].apply(lambda x: math.floor(x/7))   
    return dfapi



def plot_api_days_since_first_event_histgram(df=None):
    fix, ax=plt.subplots(figsize=(10,6))
    ax = sns.histplot(data=df['days_since_first_event'], bins=100, ax=ax)
    
    plt.show()
    
    
    
def get_api_usage_days_percentiles(df=None):
    
    df = df.groupby('id').apply(lambda x: pd.Series({
          'api_usage_days': x['timestamp'].dt.strftime('%Y%m%d').nunique()
    }))
    
    quantiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.90, 0.95, 0.99, 1]
    df = df['api_usage_days'].quantile(q=quantiles)
    
    return df
    
    

def usage_percentile(row):
    if row['api_usage_days'] <= 1:
        return ' 20th Percentile'
    elif row['api_usage_days'] <= 3:
        return ' 30th Percentile'
    elif row['api_usage_days'] <= 5:
        return ' 40th Percentile'
    elif row['api_usage_days'] <= 7:
        return ' 50th Percentile'
    elif row['api_usage_days'] <= 10:
        return ' 60th Percentile'
    elif row['api_usage_days'] <= 12:
        return ' 70th Percentile'
    elif row['api_usage_days'] <= 20:
        return ' 80th Percentile'
    elif row['api_usage_days'] <= 37:
        return ' 90th Percentile'
    elif row['api_usage_days'] <= 56:
        return ' 95th Percentile'
    elif row['api_usage_days'] <= 117:
        return ' 99th Percentile' 
    elif row['api_usage_days'] > 117:
        return '100th Percentile'
    else:
        return None
    


def assign_user_api_usage_percentile(df=None):
    df = df.groupby('id').apply(lambda x: pd.Series({
        'api_usage_days': (x['timestamp'].dt.strftime('%Y%m%d')+x['id']).nunique()
        }))
    df['api_avg_days_used_percentile'] = df.apply(lambda row: usage_percentile(row), axis=1)
    return df



def user_api_usage_agg(df=None):
    df = df.groupby('api_avg_days_used_percentile').apply(lambda x: pd.Series({
          'users': x['id'].nunique()
        , 'api_usage_days': (x['timestamp'].dt.strftime('%Y%m%d')+x['id']).nunique()
        , 'events': x['id'].count()
        , 'hours_between_events': round(x['hours_since_last_event'].mean(),2)
    }))

    df['avg_api_usage_days'] = round(df['api_usage_days'] / df['users'],1)
    df['events_per_user'] = round(df['events'] / df['users'],1)
    df['events_per_day'] = round(df['events'] / df['api_usage_days'],1)
    
    df = df[['users','events','avg_api_usage_days','events_per_user','events_per_day','hours_between_events']]   
    return df


def user_api_usage_agg_freq_split(df=None):
    
    df = df.groupby(['frequency','api_avg_days_used_percentile']).apply(lambda x: pd.Series({
          'users': x['id'].nunique()
        , 'api_usage_days': (x['timestamp'].dt.strftime('%Y%m%d')+x['id']).nunique()
        , 'events': x['id'].count()
        , 'hours_between_events': round(x['hours_since_last_event'].mean(),2)
    }))

    df['avg_api_usage_days'] = round(df['api_usage_days'] / df['users'],1)
    df['events_per_user'] = round(df['events'] / df['users'],1)
    df['events_per_day'] = round(df['events'] / df['api_usage_days'],1)
    df = df.reset_index()#.set_index('api_avg_days_used_percentile')
    df = df[['api_avg_days_used_percentile','frequency','users','events_per_user','events_per_day','hours_between_events']]  
    return df



def user_churn_df(df = None):
    churned = df.groupby('id').apply(lambda x: pd.Series({
          'D0': x[x['days_since_first_event']==0]['id'].nunique()
        , 'D1+': x[x['days_since_first_event']>=1]['id'].nunique()
        , 'D7+': x[x['days_since_first_event']>=7]['id'].nunique()
        , 'D30+': x[x['days_since_first_event']>=30]['id'].nunique()
        , 'D60+': x[x['days_since_first_event']>=60]['id'].nunique()
        , 'D90+': x[x['days_since_first_event']>=90]['id'].nunique()
    }))

    churned = churned.sum()
    churned = churned.reset_index().rename(columns={'index':'period', 0:'users_retained'})
    churned['users_churned'] = churned['users_retained'].iloc[0] - churned['users_retained']
    churned['users_churned [%]'] = round(churned['users_churned'] / churned['users_retained'].iloc[0], 4)*100
    churned['users_churned [%]'] = churned['users_churned [%]'].apply(lambda x: f'{round(x,1)}%')
    churned['users_retained [%]'] = round(churned['users_retained'] / churned['users_retained'].iloc[0], 4)*100
    churned['users_retained [%]'] = churned['users_retained [%]'].apply(lambda x: f'{round(x,1)}%')
    churned = churned.set_index('period')
    return churned


def plot_user_churn_and_retention(data=None):
    fig, [ax1, ax2] = plt.subplots(ncols=2, nrows=1, figsize=(15,6))
    sns.lineplot(data=data, x='period', y='users_retained', marker='o', ax=ax1)
    sns.lineplot(data=data, x='period', y='users_churned', marker='o', ax=ax2)
    plt.show()