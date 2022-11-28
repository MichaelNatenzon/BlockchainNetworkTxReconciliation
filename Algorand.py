#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
import numpy as np
from decimal import Decimal
from datetime import datetime

from tqdm import tqdm


# In[17]:


data_source_columns_ordering = DataSourceCols()

algo_base_adjust = 6
wallets = GetWallets()
other_party_wallets = GetExternalPartyWallets()

all_listed_wallets = wallets.copy()
all_listed_wallets.update(other_party_wallets)

network = "algo"
globals()[f"{network.lower()}_wallets"], globals()[f"{network.lower()}_wallet_names"] = ExtractWalletInfo(network, wallets)


# ## Get Current Balances

# In[252]:


def ALGOBalance(wallet_name, wallets = wallets):
    wallet_address = wallets[wallet_name]
    
    account_info = requests.get("https://algoindexer.algoexplorerapi.io/v2/accounts/{}".format(wallet_address))
    return pd.DataFrame([wallet_name, 
                         wallet_address, 
                         "ALGO", 
                         float(account_info.json()['account']['amount'])*(10**-algo_base_adjust)]).T.rename(columns={0:"Wallet",
                                                                                                                     1:"Address",
                                                                                                                     2:"Symbol",
                                                                                                                     3:"Balance"}) 


# ## Get All Transactions

# In[253]:


def ALGOTransactions(wallet_name, wallets = wallets):
    wallet_name = algo_wallet_names[0]
    wallet_address = wallets[wallet_name]
    
    r = requests.get("https://algoindexer.algoexplorerapi.io/v2/accounts/{}/transactions".format(wallet_address))

    list_of_transactions = [

      [i['fee'], i['id'], 
      i['round-time'], 
      i['payment-transaction' if 'payment-transaction' in list(i.keys()) else 'asset-transfer-transaction'], 
      i['sender'], i['tx-type']] for i in r.json()['transactions']

    ]

    transactions_df = pd.concat([pd.DataFrame(i).T for i in list_of_transactions])
    
    transactions_df = pd.concat([transactions_df.copy().reset_index(drop=True),
                             pd.DataFrame([[i['amount'], 
                                            i['receiver'], 
                                            i['asset-id'] if i.get('asset-id') else 'ALGO'] 
                                           for i in transactions_df[3]]).rename(columns={0:'amount', 
                                                                                         1:'receiver', 
                                                                                         2:'currency'}).reset_index(drop=True)], axis=1)
    
    transactions_df = transactions_df.rename(columns={0:'fee',
                                                  1:'TxnHash',
                                                  2:'Time',
                                                  4:'Sender',
                                                  5:'TxnType'})

    transactions_df = transactions_df[[i for i in transactions_df.columns if i!=3]].copy()
    
    ### Get Side Token (If Any) Details
    
    side_token_asset_ids = pd.DataFrame([[i for i in transactions_df['currency'] if i!='ALGO']]).T.rename(columns={0:'AssetID'})
    side_token_asset_ids['url'] = side_token_asset_ids['AssetID'].apply(lambda x: f"https://algoindexer.algoexplorerapi.io/v2/assets/{x}")
    side_token_asset_ids['url_return'] = side_token_asset_ids['url'].apply(lambda x: requests.get(x))
    side_token_asset_ids['url_return_df'] = side_token_asset_ids['url_return'].apply(lambda x: pd.DataFrame.from_dict({0:x.json()['asset']['params']}).T)

    side_token_asset_ids = pd.concat([side_token_asset_ids.reset_index(drop=True), 
                                      pd.concat([i for i in side_token_asset_ids['url_return_df']])], axis=1)
    
    ### Add Names for Side Tokens
    

    transactions_df = pd.merge(transactions_df.copy(), 
                               side_token_asset_ids.rename(columns={'AssetID':'currency'}),
                               how='left',
                               on='currency')

    transactions_df['currency'] = transactions_df['unit-name'].fillna(transactions_df['currency'])
    transactions_df['Wallet_Name'] = wallet_name
    
    
    transactions_df['amount'] = [(-1 if transactions_df.loc[index, 'TxnType']=='axfer' else
                                 (-1 if (transactions_df.loc[index, 'TxnType']=='pay' and transactions_df.loc[index, 'Sender']==wallets[wallet_name]) else
                                  (1 if (transactions_df.loc[index, 'TxnType']=='pay' and transactions_df.loc[index, 'receiver']==wallets[wallet_name])
                                   else 1)))*transactions_df.loc[index, 'amount']
                                 for index in transactions_df.index]

    transactions_df['TxnType'] = [('Sent' if transactions_df.loc[index, 'Sender']==transactions_df.loc[index, 'receiver'] else 'Trade') if transactions_df.loc[index, 'TxnType']=='axfer' else
                                 ('Sent' if (transactions_df.loc[index, 'TxnType']=='pay' and transactions_df.loc[index, 'Sender']==wallets[wallet_name]) else
                                  ('Received' if (transactions_df.loc[index, 'TxnType']=='pay' and transactions_df.loc[index, 'receiver']==wallets[wallet_name])
                                   else 'Other-Algo'))
                                 for index in transactions_df.index]
    
    all_transactions_output = pd.concat([
            pd.concat([i for i in transactions_df.apply(lambda x: TxnToDf(Txn(x.loc['Wallet_Name'],
                                                                                      x.loc['TxnHash'],
                                                                                      x.loc['Time'],
                                                                                      'ALGO',
                                                                                      -1*x.loc['fee'] if x.loc['TxnType']=='Sent' else 0,
                                                                                      f"{x.loc['TxnType']}-Fee",
                                                                                      x.loc['Sender'] if x.loc['Sender']==wallets[wallet_name] else x.loc['receiver'])), axis=1)]).sort_values(by='Time').reset_index(drop=True),
            pd.concat([i for i in transactions_df.apply(lambda x: TxnToDf(Txn(x.loc['Wallet_Name'],
                                                                                      x.loc['TxnHash'],
                                                                                      x.loc['Time'],
                                                                                      x.loc['currency'],
                                                                                      x.loc['amount'],
                                                                                      x.loc['TxnType'],
                                                                                      x.loc['Sender'] if x.loc['Sender']==wallets[wallet_name] else x.loc['receiver'])), axis=1)]).sort_values(by='Time').reset_index(drop=True)
    ]).reset_index(drop=True)
    
    all_transactions_output['QtyNet'] = all_transactions_output['QtyNet'] * (10**-algo_base_adjust)
    
    return all_transactions_output


# In[254]:


def ReconcileTxnsBalance(txns, balance):
    reconciled_txn_data = pd.merge(txns.groupby("Currency").agg({'QtyNet':'sum'}).reset_index().rename(columns={'QtyNet':'TxnNet', 'Currency':'Symbol'}),
                               balance, how='left', on='Symbol')

    reconciled_txn_data['NetBalance'] = reconciled_txn_data['Balance'] - reconciled_txn_data['TxnNet']
    
    add_txns_to_reconcile = reconciled_txn_data[reconciled_txn_data['NetBalance']!=0].copy()
    add_txns_to_reconcile['TxnType'] = ['Unreconcileable Received' if i>0 else 'Unreconcileable Loss' for i in add_txns_to_reconcile['NetBalance']]
    add_txns_to_reconcile = add_txns_to_reconcile[[i for i in add_txns_to_reconcile.columns if i in ['Symbol', 'NetBalance', 'TxnType']]].copy()
    add_txns_to_reconcile = add_txns_to_reconcile.rename(columns={'NetBalance':'QtyNet', 'Symbol':'Currency'})

    add_txns_to_reconcile['Time'] = 0
    add_txns_to_reconcile['Wallet'] = txns.loc[0, 'Wallet']
    add_txns_to_reconcile['WalletPair'] = " - ".join([datetime.fromtimestamp(int(min([i if i<10000000000 else int(i/1000) for i in txns['Time']]))).strftime("%Y-%m-%d"),
                                                      datetime.now().strftime("%Y-%m-%d")])
    add_txns_to_reconcile['TxnHash'] = add_txns_to_reconcile['WalletPair'].apply(lambda x: "Unreconcileable: {}".format(x))
    add_txns_to_reconcile = add_txns_to_reconcile[['Wallet', 'TxnHash', 'QtyNet', 'Currency', 'TxnType', 'Time', 'WalletPair']].copy()
    
    output = pd.concat([txns.copy(), add_txns_to_reconcile]).reset_index(drop=True)
    
    return output


# In[255]:


def AlgorandData(algo_wallet_names = algo_wallet_names, wallets = wallets):
    
    
    algo_txns = pd.DataFrame()
    algo_balances = pd.DataFrame()

    for wallet_name in algo_wallet_names:
        txns = ALGOTransactions(wallet_name)
        balances = ALGOBalance(wallet_name)

        algo_balances = pd.concat([algo_balances, balances])
        algo_txns = pd.concat([algo_txns, ReconcileTxnsBalance(txns, balances)])

    algo_balances = algo_balances.reset_index(drop=True) 
    algo_txns = algo_txns.reset_index(drop=True)  
        
    return algo_balances, algo_txns


# In[ ]:




