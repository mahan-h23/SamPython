import pandas as pd
import numpy as np


from dataCleaningModel import DataCleaning


# Reading Files
user_data = pd.read_csv('data/users.csv')

df_user = DataCleaning(user_data)
print("Users Dataframe")
df_user.showHead(10)

# Show Sum OF Null Value in Users Data
print('Number of Null Values in Users Data:\n',df_user.nullNumber())

# Fill Null Value in Affiliate With Others
df_user.fillNaText('affiliate','Others')

# Fill Null Value in OS Name With Others
df_user.fillNaText('os_name','Others')


# Number of users based on their OS
usersOs = df_user.groupByCount('os_name','user_id')
print('Users OS:\n',usersOs)

# Number of users based on their Phone operator
usersPhoneOperator= df_user.groupByCount('phone_operator','user_id')
print('Users Phone Operator:\n',usersPhoneOperator)


# Number of users based on their Services
usersServices = df_user.groupByCount('service','user_id')
print('Users Services:\n',usersServices)


# -----------------

transaction_data = pd.read_csv('data/transactions.tsv', sep='\t', header=0)




df_transaction = DataCleaning(transaction_data)
print("Transaction dataframe:")
df_transaction.showHead(10)
# Show Sum OF Null Value in Transaction Data
print('Number of Null Values in Transaction Data:\n',df_transaction.nullNumber())

# Fill Null Value in Status With Others
df_transaction.fillNaText('status','Others')
# -----------------


# Drop Unnecessary Columns

# In users file (Aggregator) column is duplicate
df_user.dropColumn('aggregator')

# In Transaction file we don't need ( Services, phone operator) because they duplicate in user file and (price point)
df_transaction.dropColumn('pricepoint')
df_transaction.dropColumn('phone_operator')
df_transaction.dropColumn('service')

# -----------------------
# Turn to date format in order to calculate time delta
df_user.toDateTime('subscription_date')
df_user.toDateTime('unsubscription_date')
# Create column for time dif between subscription_date and unsubscription_date
df_user.df['active_duration'] = df_user.df['unsubscription_date'] - df_user.df['subscription_date']
print("********************")
# ------------- Segment Users based on their active duration

# Active for less than 5 days

active_less_5d = df_user.df.loc[df_user.df["active_duration"] <= pd.Timedelta(days=5)]["user_id"].count()
print("Users Active less Than 5 days:\n ",active_less_5d)

# Active for between 5 to 10 days

active_between_5to10 = df_user.df.loc[(df_user.df["active_duration"] > pd.Timedelta(days=5)) & (df_user.df["active_duration"] <= pd.Timedelta(days=10))]["user_id"].count()
print("Users Active for 5 to 10 days:\n ",active_between_5to10)

# Active for between 10 to 15 days

active_between_10to15 = df_user.df.loc[(df_user.df["active_duration"] > pd.Timedelta(days=10)) & (df_user.df["active_duration"] <= pd.Timedelta(days=15))]["user_id"].count()
print("Users Active for 10 to 15 days:\n ",active_between_10to15)

# Active for between 15 to 30 days

active_between_15to30 = df_user.df.loc[(df_user.df["active_duration"] > pd.Timedelta(days=15)) & (df_user.df["active_duration"] <= pd.Timedelta(days=30))]["user_id"].count()
print("Users Active for 15 to 30 days:\n ",active_between_15to30)

# affiliate loss Users in a month

affiliate_loss_1M = df_user.df.loc[ (df_user.df["active_duration"] <= pd.Timedelta(days=30))]
df_affiliate_loss_1M = DataCleaning(affiliate_loss_1M)
aff_1m = df_affiliate_loss_1M.groupByCount('affiliate','user_id')

print('Affiliate Users Lost Less than a month: \n',aff_1m)

# -------------------------------------

# If active_duration is NaT so to user is active, so in this step we find active users
pd.set_option('mode.chained_assignment', None)  # To copy on column without warning
df_user.df['is_active'] = 0
for i in range(0, len(df_user.df['user_id'])):
    if df_user.df['active_duration'].loc[i] is pd.NaT:
        df_user.df['is_active'].loc[i] = 1



# --------------------------------
# Merging to data with Inner Join based on user id to find each transaction time
merge_data = pd.merge(df_user.df, df_transaction.df, on='user_id')
df_m = DataCleaning(merge_data)
df_m.showHead(10)
# Turn (subscription_date, unsubscription_date, transaction_timestamp) to date format in order to calculate time delta
df_m.toDateTime('transaction_timestamp')

# Calculate time delta between last transaction_timestamp and unsubscription_date
df_m.df['unsub_to_last_trans'] = df_m.df['unsubscription_date'] - df_m.df['transaction_timestamp']

# Number of Active and Deactivated Users
print('Number of Active and Deactivate Users: \n', df_user.groupByCount('is_active', 'user_id'))
print("********************")
# Unique affiliate we have in data
print('List Of Affiliate: \n', df_m.uniques('affiliate'))
print("********************")

# Number of active and Deactivated Users Based On Affiliate
usersByAffiliate =  df_user.df.groupby(['affiliate','is_active'], as_index=False)["user_id"].count()
print('Active and Deactivated Users based on Affiliate: \n', usersByAffiliate)
print("********************")

# Number of users based on active and deactivated by services and affiliate


# Unique Services we have in data
print('List Of Services: \n', df_m.uniques('service'))
print("********************")
# Unique Phone Operator we have in data
print('List Of Phone Operator: \n', df_m.uniques('phone_operator'))
print("********************")

# Find Target Customer OS to improve software
customerOS = df_user.groupByCount('os_name', 'user_id')
print("Customer OS \n", customerOS)
print("********************")


# Status Overview per transaction
statusOverview = df_m.groupByCount('status','user_id')



# Number of Status per Transaction and phone operator
phoneByOperator = df_m.df.groupby(['status', "phone_operator"], as_index=False)["user_id"].count()
print("Phone Operator Status per Transaction\n", phoneByOperator)
print("********************")

# Number of Status per Transaction and phone operator and their OS
phoneByOperatorOS = df_m.df.groupby(["os_name", 'status', "phone_operator"], as_index=False)["user_id"].count()
print("Phone Operator Status per Transaction and their OS\n", phoneByOperatorOS)
print("********************")

# All of Transactions Based on Affiliates
allAffiliate = df_m.groupByCount('affiliate', 'user_id')
print("All Of transactions based on Affiliate\n", allAffiliate)
print("********************")

# Find Most Failed Affiliate
failedAffiliate = df_m.df.groupby(["affiliate", 'status'], as_index=False)["user_id"].count()
print("Transaction Status per \n", failedAffiliate)
print("********************")

# Number of Users that lead to unsubscribe after fail less than 1 hour
df_unsub_less_1H = df_m.df.loc[
    (df_m.df['status'].astype(str) == 'Failed') & (df_m.df['unsub_to_last_trans'] <= pd.Timedelta(hours=1))]
numberOfUniqueUsers = len(pd.unique(df_unsub_less_1H['user_id']))
print('Number of Users That Unsubscribe less than an Hour after fail payment:\n', numberOfUniqueUsers)
print("********************")

# Number of Users that lead to unsubscribe after fail between 1 to 5 hours
df_unsub_between_2_5 = df_m.df.loc[
    (df_m.df['status'].astype(str) == 'Failed') & (df_m.df['unsub_to_last_trans'] > pd.Timedelta(hours=1)) & (
                df_m.df['unsub_to_last_trans'] <= pd.Timedelta(hours=5))]
numberOfUniqueUsers2To5 = len(pd.unique(df_unsub_between_2_5['user_id']))
print('Number of Users That Unsubscribe between 2 to 5 Hours after fail payment:\n', numberOfUniqueUsers2To5)
print("********************")


# --- Segment user lost based in Failed Payment they had--------------------------
# Less than an hour
df_m.df['lost_less1H'] = 0
for i in range(0, len(df_m.df['user_id'])):
    if df_m.df['unsub_to_last_trans'].loc[i] <= pd.Timedelta(hours=1):
        df_m.df['lost_less1H'].loc[i] = 1
print("Deactivated Users in one hours after failed Payment:\n",df_m.SumColumn('lost_less1H'))
print("********************")
# between 1 to 5 hours
df_m.df['lost_between_1h_5h'] = 0
for i in range(0, len(df_m.df['user_id'])):
    if df_m.df['unsub_to_last_trans'].loc[i] > pd.Timedelta(hours=1) and df_m.df['unsub_to_last_trans'].loc[i] <= pd.Timedelta(hours=5) :
        df_m.df['lost_between_1h_5h'].loc[i] = 1

print("Deactivated Users between 1 to 5 hours after failed Payment:\n",df_m.SumColumn('lost_between_1h_5h'))
print("********************")
# between 5 to 24 hours
df_m.df['lost_between_5h_24h'] = 0
for i in range(0, len(df_m.df['user_id'])):
    if df_m.df['unsub_to_last_trans'].loc[i] > pd.Timedelta(hours=5) and df_m.df['unsub_to_last_trans'].loc[i] <= pd.Timedelta(hours=24) :
        df_m.df['lost_between_5h_24h'].loc[i] = 1

print("Deactivated Users between 5 to 24 hours after failed Payment:\n",df_m.SumColumn('lost_between_5h_24h'))
print("********************")