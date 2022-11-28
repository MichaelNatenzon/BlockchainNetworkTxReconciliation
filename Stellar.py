#!/usr/bin/env python
# coding: utf-8

# In[4]:


try:
    from Classes.ObjectClasses import Txn
    from Price import DateCoinGecko
    from GeneralFunctions import (GetDictKey, 
                                  GetWallets,
                                  GetExternalPartyWallets,
                                  ExtractWalletInfo, 
                                  ConvertTimestampToUnixTime, 
                                  FormatTimeColInDf, 
                                  DataSourceCols,
                                  TxnToDf)
except:
    from Network_Integration.Classes.ObjectClasses import Txn
    from Network_Integration.Price import DateCoinGecko
    from Network_Integration.GeneralFunctions import (GetDictKey, 
                                                      GetWallets,
                                                      GetExternalPartyWallets,
                                                      ExtractWalletInfo, 
                                                      ConvertTimestampToUnixTime, 
                                                      FormatTimeColInDf, 
                                                      DataSourceCols,
                                                      TxnToDf)


# In[2]:


import os
import time
import re
import requests
import pandas as pd
from datetime import datetime


# In[5]:


data_source_columns_ordering = DataSourceCols()

xlm_base_adjust = 1
wallets = GetWallets()
other_party_wallets = GetExternalPartyWallets()

all_listed_wallets = wallets.copy()
all_listed_wallets.update(other_party_wallets)

network = "xlm"
globals()[f"{network.lower()}_wallets"], globals()[f"{network.lower()}_wallet_names"] = ExtractWalletInfo(network, wallets)


# ## Get Current Balances

# In[116]:


def XLMBalance(wallet_name, wallets = wallets):
    wallet_address = wallets[wallet_name]
    account_info = requests.get(f"https://horizon.stellar.org/accounts/{wallet_address}")
    account_balances = pd.concat([pd.DataFrame.from_dict({index:i}).T for index, i in enumerate(account_info.json()['balances'])])
    account_balances = account_balances[['asset_type', 'balance']].rename(columns={'asset_type':'Symbol', 'balance':'Balance'})
    account_balances['Symbol'] = [i if i!='native' else 'XLM' for i in account_balances['Symbol']]
    account_balances['Balance'] =  account_balances['Balance'].astype(float)
    return pd.concat([pd.DataFrame([wallet_name, wallet_address]).T.rename(columns={0:'Wallet', 1:'Address'}), account_balances], axis=1)


# ## Get All Transactions

# In[108]:


def XLMTransactions(wallet_name, wallets = wallets):
    wallet_address = wallets[wallet_name]
    

    wallet_payments = requests.get(f"https://horizon.stellar.org/accounts/{wallet_address}/payments?order=desc")
    results = pd.concat([pd.DataFrame.from_dict(i) for i in wallet_payments.json()["_embedded"]["records"]])
    results = results[[i for i in results.columns if i!='_links']].drop_duplicates().reset_index(drop=True)
    results = results[results['transaction_successful']==True].reset_index(drop=True)
    results['amount'] = results['amount'].fillna(results['starting_balance'])
    results['to'] = results['to'].fillna(results['account'])
    results['from'] = results['from'].fillna(results['funder'])
    
    wallet_payments = requests.get(f"https://horizon.stellar.org/accounts/{wallet_address}/transactions")
    
    results = pd.merge(results.copy(), 
                       pd.DataFrame([[i['hash'], float(i['fee_charged'])/(10**7)] for i in wallet_payments.json()['_embedded']['records']]).rename(columns={0:'transaction_hash', 1:'Fee'}),
                       how='left', 
                       on='transaction_hash')
    
    
    output = pd.concat([
        pd.concat([i for i in results.reset_index()['index'].apply(lambda x: TxnToDf(Txn(wallet_name,
                                                                                results.loc[x,'transaction_hash'],
                                                                                int(pd.Timestamp(results.loc[x,'created_at']).timestamp()),
                                                                                "XLM",
                                                                                float(results.loc[x,'amount']) if results.loc[x,'to'].lower() == wallet_address.lower() else -1*float(results.loc[x,'amount']),
                                                                                "Received" if results.loc[x,'to'].lower() == wallet_address.lower() else "Sent",
                                                                                results.loc[x,'from'] if results.loc[x,'to'].lower() == wallet_address.lower() else results.loc[x,'to'])))]),
        pd.concat([i for i in results.reset_index()['index'].apply(lambda x: TxnToDf(Txn(wallet_name,
                                                                                results.loc[x,'transaction_hash'],
                                                                                int(pd.Timestamp(results.loc[x,'created_at']).timestamp()),
                                                                                "XLM",
                                                                                0 if results.loc[x,'to'].lower() == wallet_address.lower() else -1*float(results.loc[x,'Fee']),
                                                                                "Receive-Fee" if results.loc[x,'to'].lower() == wallet_address.lower() else "Sent-fee",
                                                                                results.loc[x,'from'] if results.loc[x,'to'].lower() == wallet_address.lower() else results.loc[x,'to'])))])
    ])
    
    

    return output.sort_values(by="TxnHash").reset_index(drop=True).reset_index(drop=True)


# In[130]:


def XLMReconcile(xlm_txns, xlm_balance):
    reconciled_txn_data = pd.merge(xlm_txns.groupby("Currency").agg({'QtyNet':'sum'}).reset_index().rename(columns={'QtyNet':'TxnNet', 'Currency':'Symbol'}),
                               xlm_balance, how='left', on='Symbol')

    reconciled_txn_data['NetBalance'] = reconciled_txn_data['Balance'] - reconciled_txn_data['TxnNet']
    
    add_txns_to_reconcile = reconciled_txn_data[reconciled_txn_data['NetBalance']!=0].copy()
    add_txns_to_reconcile['TxnType'] = ['Unreconcileable Received' if i>0 else 'Unreconcileable Loss' for i in add_txns_to_reconcile['NetBalance']]
    add_txns_to_reconcile = add_txns_to_reconcile[[i for i in add_txns_to_reconcile.columns if i in ['Symbol', 'NetBalance', 'TxnType']]].copy()
    add_txns_to_reconcile = add_txns_to_reconcile.rename(columns={'NetBalance':'QtyNet', 'Symbol':'Currency'})

    add_txns_to_reconcile['Time'] = 0
    add_txns_to_reconcile['Wallet'] = xlm_txns.loc[0, 'Wallet']
    add_txns_to_reconcile['WalletPair'] = " - ".join([datetime.fromtimestamp(int(min([i if i<10000000000 else int(i/1000) for i in xlm_txns['Time']]))).strftime("%Y-%m-%d"),
                                                      datetime.now().strftime("%Y-%m-%d")])
    add_txns_to_reconcile['TxnHash'] = add_txns_to_reconcile['WalletPair'].apply(lambda x: "Unreconcileable: {}".format(x))
    add_txns_to_reconcile = add_txns_to_reconcile[['Wallet', 'TxnHash', 'QtyNet', 'Currency', 'TxnType', 'Time', 'WalletPair']].copy()
    
    output = pd.concat([xlm_txns.copy(), add_txns_to_reconcile]).reset_index(drop=True)
    
    return output


# In[131]:


xlm_txns = pd.DataFrame()
xlm_balances = pd.DataFrame()

for wallet_name in xlm_wallet_names:
    txns = XLMTransactions(wallet_name)
    balances = XLMBalance(wallet_name)
    
    xlm_balances = pd.concat([xlm_balances, balances])
    xlm_txns = pd.concat([xlm_txns, XLMReconcile(txns, balances)])

xlm_balances = xlm_balances.reset_index(drop=True) 
xlm_txns = xlm_txns.reset_index(drop=True)  


# In[24]:


def XLMData(xlm_balances = xlm_balances, xlm_txns = xlm_txns):
    return xlm_balances, xlm_txns

