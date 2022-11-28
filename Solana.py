#!/usr/bin/env python
# coding: utf-8

# In[1]:


try:
    from Classes.ObjectClasses import Txn
    from Price import DateCoinGecko
    from GeneralFunctions import (GetDictKey, 
                                  GetWallets,
                                  GetExternalPartyWallets,
                                  GetContracts,
                                  GetTokenContracts,
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
                                                      GetContracts,
                                                      GetTokenContracts,
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
from solana.rpc.api import Client

import solana.rpc as solrpc


# In[3]:


data_source_columns_ordering = DataSourceCols()

wallets = GetWallets()
other_party_wallets = GetExternalPartyWallets()

all_listed_wallets = wallets.copy()
all_listed_wallets.update(other_party_wallets)

network = "sol"

globals()[f"{network.lower()}__base_adjust"] = 6
globals()[f"{network.lower()}_wallets"], globals()[f"{network.lower()}_wallet_names"] = ExtractWalletInfo(network, wallets)


# ## Get Transactions
# Will get balances after (Need to feed in txn data to get SLP token balances)

# In[4]:


def ExtractTransactionDetails(final_dict, wallet_address, txn_hash, event_time, fee_amount):

    overheads = pd.DataFrame()

    pre_post_balances = pd.DataFrame(
        [['SOL' for i in range(len(final_dict['result']['meta']['preBalances']))],
         final_dict['result']['transaction']['message']['accountKeys'],
         final_dict['result']['meta']['preBalances'],
         final_dict['result']['meta']['postBalances']
        ]).T.rename(columns={0:'Currency',1:"WalletPair",2:"Pre",3:"Post"})

    pre_post_balances['Base'] = 9

    if len(final_dict['result']['meta']['preTokenBalances'])>0:
        pre_balance = pd.DataFrame([[i[j] for j in i.keys() if j!='uiTokenAmount']+list(i['uiTokenAmount'].values())[0:2] for i in final_dict['result']['meta']['preTokenBalances']])

    else:
        pre_balance = pd.DataFrame(columns = [i for i in range(5)])

    if len(final_dict['result']['meta']['postTokenBalances'])>0:
        post_balance =  pd.DataFrame([[i[j] for j in i.keys() if j!='uiTokenAmount']+list(i['uiTokenAmount'].values())[0:2] for i in final_dict['result']['meta']['postTokenBalances']])
    else:
        post_balance = pd.DataFrame(columns = [i for i in range(5)])

    token_balances = pd.merge(
        pre_balance,
        post_balance,
        how='right',
        on=[0, 1, 2])

    token_balances['4_y'] = token_balances['4_y'].fillna(token_balances['4_x'])
    token_balances = token_balances[[i for i in token_balances.columns if i!='4_x']].copy()
    token_balances['3_x'] = token_balances['3_x'].fillna(0)



    token_balances = token_balances[[i if re.search("_", i) else int(i) for i in sorted([str(i) for i in token_balances.columns if i!=0])]].copy()
    token_balances.columns = pre_post_balances.columns

    pre_post_balances = pd.concat([pre_post_balances.copy(),
                                   token_balances.copy()
                                   ]).reset_index(drop=True)

    pre_post_balances['Post'] = pre_post_balances['Post'].fillna(0).astype('int64')
    pre_post_balances['Pre'] = pre_post_balances['Pre'].fillna(0).astype('int64')
    pre_post_balances['QtyNet'] = pre_post_balances['Post'] - pre_post_balances['Pre']

    pre_post_balances = pre_post_balances[abs(pre_post_balances['QtyNet']!=0)].copy()
    pre_post_balances['TxnType'] = ["Sent" if i<0 else "Received" for i in pre_post_balances['QtyNet']]

    pre_post_balances['Wallet'] = wallet_address
    pre_post_balances['TxHash'] = txn_hash
    pre_post_balances['Time'] = event_time

    # pre_post_balances['Currency'] = pre_post_balances['Currency'].copy().str.replace("SOL", "So11111111111111111111111111111111111111112")

    # Pull Out Other Wallets to Do Some Processing Later
    other_wallets = pre_post_balances[(pre_post_balances['WalletPair'].isin([wallet_address])==False)
                                      & (abs(pre_post_balances['QtyNet'])!=0)].copy().reset_index(drop=True)
    other_wallets = other_wallets[[i for i in other_wallets.columns if i not in ["Pre", "Post"]]].copy()


    pre_post_balances = pre_post_balances[((pre_post_balances['WalletPair'].isin([wallet_address])))].copy().reset_index(drop=True)

    pre_post_balances['QtyNet'] = [int(pre_post_balances.loc[index, 'QtyNet'])
                                   if pre_post_balances.loc[index, 'WalletPair']==pre_post_balances.loc[index, 'Wallet']
                                   else -1*int(pre_post_balances.loc[index, 'QtyNet'])
                                   for index in pre_post_balances.index]

    pre_post_balances['TxnType'] = ['Sent' if i<0 else 'Received' for i in pre_post_balances['QtyNet']]




    col_names = ['Currency', 'WalletPair', 'QtyNet', 'Base', 'Wallet',  'Time', 'TxHash', 'TxnType']

    pre_post_balances = pre_post_balances[col_names].copy()
    pre_post_balances.columns = [i for i in range(len(pre_post_balances.columns))]

    # Add Tx Fees and Overhead Fees


    # Get Any "Overhead" Expenses
    if len(final_dict['result']['meta']['rewards'])>0:
        overheads = pd.concat([pd.DataFrame.from_dict({index: i}).T for index, i in enumerate(final_dict['result']['meta']['rewards'])])
        overheads['commission'] = overheads['commission'].fillna(0)
        overheads['rewardType'] = overheads['rewardType'] + '-Fee'
        overheads['Currency'] = "SOL"
        overheads['Base'] = 9
        overheads['Wallet'] = wallet_address
        overheads['Time'] = event_time
        overheads['TxHash'] = txn_hash

        overheads = overheads[["Currency", 
                               "pubkey", 
                               "lamports", 
                               "Base", 
                               "Wallet", 
                               "Time", 
                               "TxHash", 
                               "rewardType"]].copy()
        overheads['lamports'] = abs(overheads['lamports'])
        overheads.columns = [i for i in range(len(overheads.columns))]



    # Add Send Fee to Transaction
    fee_txns = pd.concat([
        pd.DataFrame([
            ['SOL', 
             "Other", 
             abs(fee_amount),
             9, 
             wallet_address, 
             event_time, 
             txn_hash,
             'Sent-Fee']]),
        overheads
    ]).reset_index(drop=True)

    pre_post_balances = pd.concat([pre_post_balances.copy(),
                               fee_txns
                              ]).reset_index(drop=True)

    pre_post_balances.columns = col_names

    # Remove wSOL since it's accounted for in the SOL balances

    output = pd.concat([other_wallets, pre_post_balances]).reset_index(drop=True)
    output = output[output['Currency']!='So11111111111111111111111111111111111111112'].copy().reset_index(drop=True)


    # Fill in the correct wallet pair for the sent-fee
    sol_only = output[output['Currency']=='SOL'].copy()
    tmp_aggragates = sol_only.groupby(["Currency"]).agg({"QtyNet":"sum"}).reset_index()


    # If aggregate is equal to zero, it means the transaction was a contract interaction or an error (ie. Fee quantity was the total quantity out)
    if tmp_aggragates.loc[0, 'QtyNet']==0:

        output.loc[list(sol_only[sol_only['TxnType']=="Sent"].index)[0], "QtyNet"] = (sol_only[sol_only['TxnType']=="Sent"]['QtyNet'].iloc[0] 
                                                                                      + sol_only[sol_only['TxnType'].str.contains('Fee')]['QtyNet'].sum())
        output.loc[sol_only[sol_only['TxnType'].str.contains("Fee")].index, "QtyNet"] = -1 * output.loc[sol_only[sol_only['TxnType'].str.contains("Fee")].index, "QtyNet"]

    else:
        tmp_sent_sol = tmp_aggragates[tmp_aggragates['QtyNet']<0].copy()
        tmp_received_sol = tmp_aggragates[(tmp_aggragates['QtyNet']>0) & (tmp_aggragates['WalletPair']!="Other")].copy()

        fee_indices = sol_only[sol_only['TxnType'].str.contains('Fee')].index
        fee_qty =  sol_only[sol_only['TxnType'].str.contains('Fee')]['QtyNet'].sum()

        # Since sender pays the fees
        if tmp_received_sol['QtyNet'].sum() == abs(tmp_sent_sol['QtyNet'].sum()) - abs(fee_qty):


            output.loc[sol_only[sol_only['TxnType'].str.contains("Fee")].index, "QtyNet"] = -1 * output.loc[sol_only[sol_only['TxnType'].str.contains("Fee")].index, "QtyNet"]


            # If own wallet is sender
            if wallet_address in tmp_sent_sol['WalletPair'].to_list():
                if len(tmp_sent_sol)>1:
                    print("Error: Multiple Senders. Who Paid Fee?")
                else:
                    output.loc[fee_indices, 'WalletPair'] = wallet_address

                    adjust_sent_indices = output[(output["Currency"]=="SOL") 
                                                 & (output["WalletPair"]==wallet_address)
                                                 & (output["TxnType"]=="Sent")
                                                ].index

                    output.loc[adjust_sent_indices, 'QtyNet'] = output.loc[adjust_sent_indices, 'QtyNet'] + abs(output[output['TxnType'].str.contains('Fee')]['QtyNet'].sum())


            # If other wallet is sender
            elif wallet_address in tmp_received_sol['WalletPair'].to_list():

                if len(tmp_sent_sol)>1:
                    print("Error: Multiple Senders. Who Paid Fee?")
                else:
                    output.loc[fee_indices, 'WalletPair'] = wallet_address
                    output.loc[fee_indices, 'QtyNet'] = 0

                    adjust_sent_indices = output[(output["Currency"]=="SOL") 
                                                 & (output["TxnType"]=="Sent")
                                                ].index

                    output.loc[adjust_sent_indices, 'QtyNet'] = output.loc[adjust_sent_indices, 'QtyNet'] + abs(output[output['TxnType'].str.contains('Fee')]['QtyNet'].sum())

            else:
                print("ERROR: Cant Find Which Wallet Paid Fee")

        else:
            print("ERROR: Fees Not Linking Up With Net Balances")


        # Check That Totals Match As They Should
        if abs(output[output['Currency']=="SOL"].groupby("Currency").agg({"QtyNet":"sum"}).loc['SOL', 'QtyNet']) != abs(fee_qty):
                print("Error: SOL Aggregates Mismatch")



    self_txns = output[(output['WalletPair']==wallet_address) & (output['TxnType'].isin(['Sent', 'Received']))].copy().reset_index()
    other_txns = output[(output['WalletPair']!=wallet_address) & (output['TxnType'].isin(['Sent', 'Received']))][['WalletPair', 'Currency', 'QtyNet']].copy()

    other_txns['QtyNet'] = -1*other_txns['QtyNet']

    new_walletpairs = pd.merge(self_txns, other_txns, how='left', on=['Currency', 'QtyNet'])
    new_walletpairs['WalletPair'] = new_walletpairs['WalletPair_y'].fillna(''.join([str(0) for i in range(44)]))

    output.loc[list(new_walletpairs['index']), "WalletPair"] = [i for i in new_walletpairs['WalletPair'].copy()]

    txn_types = self_txns[self_txns['TxnType'].str.contains("Fee")==False]['TxnType'].drop_duplicates().to_list()
    
    # Determine if a staking transaction
    if len(final_dict['result']['meta']['logMessages'])>0:
        stake_num = sum([1 if re.search("stake", i.lower()) else 0 for i in final_dict['result']['meta']['logMessages']])
    else:
        stake_num = 0
        

    # Sending and receiving a stake is a transaction cost to me in both cases
    if stake_num>0:
        full_output = pd.concat([output[output['TxnType'].str.contains("Fee")], 
                                 new_walletpairs[output.columns].copy()]).reset_index(drop=True)  
        
        # Label Staking Transaction Fees
        full_output['TxnType'] = ["StakeTxn-Fee" if (i.lower() in ['sent-fee', 'received-fee'])
                                  else f"{i}-Stake" for i in full_output['TxnType']]
    
    # If only received crypto, couldn't have paid fee
    elif (len(txn_types)==1 and txn_types[0]=='Received'):
        full_output = new_walletpairs[output.columns].copy()
        
    # If only received sol, couldn't have paid fee
    elif (len(self_txns[(self_txns['TxnType'].str.contains("Fee")==False) & (self_txns['Currency']=="SOL")]['TxnType'].drop_duplicates())==1 
          and self_txns[(self_txns['TxnType'].str.contains("Fee")==False) & (self_txns['Currency']=="SOL")]['TxnType'].drop_duplicates().to_list()[0]=='Received'):
        full_output = new_walletpairs[output.columns].copy()
        
    else:
        full_output = pd.concat([output[output['TxnType'].str.contains("Fee")], 
                             new_walletpairs[output.columns].copy()]).reset_index(drop=True)

    full_output.loc[full_output[full_output['WalletPair'].isin(['Other', wallet_address])].index, "WalletPair"] = ''.join([str(0) for i in range(44)])     
    
    full_output['QtyNet'] = full_output['QtyNet'] / (10**full_output['Base'].astype(int))
    full_output = full_output[[i for i in full_output.columns if i !='Base']].copy()
    full_output = full_output[abs(full_output['QtyNet'])!=0].reset_index(drop=True)
    
    # Label transaction if was an error
    if final_dict['result']['meta']['err']:
        full_output.loc[full_output[full_output['TxnType'].str.contains("Fee")].index, "TxnType"] = "Error-Fee" 
    
    
    
    
    
    get_token_contracts = GetTokenContracts()[network.upper()]
    
    full_output['Currency'] = [get_token_contracts.get(i) if get_token_contracts.get(i) else i for i in full_output['Currency']] 
    
    full_output['Wallet'] = full_output['Wallet'].map({v: k for k, v in wallets.items()})
    
    all_transactions_output = pd.concat([
            pd.concat([i for i in full_output.apply(lambda x: TxnToDf(Txn(x.loc['Wallet'],
                                                                     x.loc['TxHash'],
                                                                     x.loc['Time'],
                                                                     x.loc['Currency'],
                                                                     x.loc['QtyNet'],
                                                                     x.loc['TxnType'],
                                                                     x.loc['WalletPair'])), axis=1)]).sort_values(by='Time').reset_index(drop=True)
    ]).reset_index(drop=True)
    
    
    
    return all_transactions_output


# In[5]:


def SOLTransactions(wallet_name, wallets = wallets):

    wallet_address = wallets[wallet_name]

    http_client = Client("https://api.mainnet-beta.solana.com")

    all_transactions_raw = http_client.get_signatures_for_address(wallet_address) 
    all_transactions = pd.concat([pd.DataFrame.from_dict({index:i}).T for index, i in enumerate(all_transactions_raw['result'])])


    tx_details = []
    for index_val in tqdm(all_transactions.index):
        tx_details.append(http_client.get_transaction(all_transactions.loc[index_val, 'signature']))

    all_transactions = pd.concat([all_transactions.copy(), 
                                  pd.Series(tx_details)], axis=1).rename(columns={0:'details'})

    all_transactions['fee'] = all_transactions['details'].apply(lambda x: x['result']['meta']['fee'])
    all_transactions['Wallet'] = wallet_address
    
    output_txn_details = []
    for index_val in all_transactions.index:
    
        final_dict = all_transactions.loc[index_val, 'details'].copy()
        txn_hash = all_transactions.loc[index_val, 'signature']
        event_time = all_transactions.loc[index_val, 'blockTime']
        fee_amount = all_transactions.loc[index_val, 'fee']

        output_txn_details.append(ExtractTransactionDetails(final_dict, wallet_address, txn_hash, event_time, fee_amount))
        
    return pd.concat([i for i in output_txn_details]).reset_index(drop=True)


# ## Get Current Balances

# In[6]:


def SOLBalance(wallet_name, transactions, wallets = wallets):
    
    
    wallet_address = wallets[wallet_name]
    known_tokens = GetTokenContracts()[network.upper()]
    
    http_client = Client("https://api.mainnet-beta.solana.com")
    
    retrieve_tokens = [j 
                   for j in [({v: k for k, v in known_tokens.items()}).get(i) 
                             if ({v: k for k, v in known_tokens.items()}).get(i) 
                             else i for i in transactions['Currency'].drop_duplicates().to_list()] if j not in ['SOL', 'So11111111111111111111111111111111111111112']]
    
    token_balances = []
    for token in retrieve_tokens:
        d = (http_client.get_token_accounts_by_owner(wallet_address, solrpc.types.TokenAccountOpts(token)))
        
        token_balances.append(http_client.get_token_account_balance(d['result']['value'][0]['pubkey']))
    
    if len(token_balances)>0:
        
        spl_balances = pd.concat([
            pd.concat([pd.DataFrame.from_dict({index:i['result']['value']}).T for index, i in enumerate(token_balances)]),
            pd.DataFrame(retrieve_tokens)
        ], axis=1)[['amount', 'decimals', 0]].rename(columns={0:'Contract'})

        spl_balances['Symbol'] = [known_tokens.get(i) if known_tokens.get(i) else i for i in spl_balances['Contract']]
        spl_balances['Balance'] = spl_balances['amount'].astype(float) / (10**spl_balances['decimals'].astype(int))
        spl_balances['Wallet'] = wallet_name
        spl_balances['Address'] = wallet_address

        spl_balances = spl_balances[['Wallet', 'Address', 'Symbol', 'Balance']].copy()


        sol_balance = http_client.get_balance(wallet_address)

        output = pd.concat([spl_balances,
                            pd.DataFrame([wallet_name, 
                                          wallet_address, 
                                          "SOL", 
                                          float(sol_balance['result']['value'])*(10**-9)]).T.rename(columns={0:"Wallet",
                                                                                                             1:"Address",
                                                                                                             2:"Symbol",
                                                                                                             3:"Balance"})]).reset_index(drop=True)
    else:
        sol_balance = http_client.get_balance(wallet_address)
        
        output = pd.DataFrame([wallet_name, 
                                          wallet_address, 
                                          "SOL", 
                                          float(sol_balance['result']['value'])*(10**-9)]).T.rename(columns={0:"Wallet",
                                                                                                             1:"Address",
                                                                                                             2:"Symbol",
                                                                                                             3:"Balance"}).reset_index(drop=True)
        
    return output


# ## Reconcile Transactions

# In[7]:


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


# In[8]:


def SOLData(sol_wallet_names = sol_wallet_names, wallets = wallets):
    
    sol_txns = pd.DataFrame()
    sol_balances = pd.DataFrame()

    for wallet_name in sol_wallet_names:
        txns = SOLTransactions(wallet_name)
        balances = SOLBalance(wallet_name, txns)

        sol_balances = pd.concat([sol_balances, balances])
        sol_txns = pd.concat([sol_txns, ReconcileTxnsBalance(txns, balances)])

    sol_balances = sol_balances.reset_index(drop=True) 
    sol_txns = sol_txns.reset_index(drop=True)
    
    known_contracts = GetContracts()
    known_contracts = known_contracts["SOL"]
    
    sol_txns['WalletPair'] = [({v: k for k, v in other_party_wallets.items()}).get(i) 
                              if ({v: k for k, v in other_party_wallets.items()}).get(i)
                              else i for i in sol_txns['WalletPair']]
    
    sol_txns['Currency'] = [({v: k for k, v in known_contracts.items()}).get(i)
                            if ({v: k for k, v in known_contracts.items()}).get(i)
                            else i for i in sol_txns['Currency']]
        
    return sol_balances, sol_txns

