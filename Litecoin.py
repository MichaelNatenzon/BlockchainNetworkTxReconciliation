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


# In[25]:


import os
import time
import re
import requests
import pandas as pd
import numpy as np
from decimal import Decimal
from datetime import datetime

from tqdm import tqdm


# In[3]:


data_source_columns_ordering = DataSourceCols()

ltc_base_adjust = 8
wallets = GetWallets()
other_party_wallets = GetExternalPartyWallets()

all_listed_wallets = wallets.copy()
all_listed_wallets.update(other_party_wallets)

network = "ltc"
globals()[f"{network.lower()}_wallets"], globals()[f"{network.lower()}_wallet_names"] = ExtractWalletInfo(network, wallets)


# ## Get Current Balances

# In[4]:


def LTCBalance(wallet_name, wallets = wallets):
    wallet_address = wallets[wallet_name]
    
    try:
        account_info = requests.get(f"https://api.blockchair.com/litecoin/dashboards/address/{wallet_address}")
        account_info = account_info.json()
        return pd.concat([pd.DataFrame([wallet_name, wallet_address]).T.rename(columns={0:'Wallet', 1:'Address'}), 
                          pd.DataFrame(["LTC", float(account_info['data'][wallet_address]['address']['balance'])*10**-8]).T.rename(columns={0:'Symbol', 1:'Balance'})], axis=1)
    
    except:
        account_info = requests.get(f"https://chain.so/api/v2/get_address_balance/LTC/{wallet_address}")
        return pd.DataFrame([wallet_name, wallet_address, "LTC", float(account_info.json()['data']['confirmed_balance'])]).T.rename(columns={0:"Wallet",
                                                                                                                                             1:"Address",
                                                                                                                                             2:"Symbol",
                                                                                                                                             3:"Balance"})        


# ## Get All Transactions

# In[5]:


def LtcFindNewLedgerWallets(ltc_wallets = ltc_wallets.copy(), 
                            other_party_wallets = other_party_wallets.copy(),
                            wallets = wallets.copy()):
    
    wallet_keys = dict((v,k) for k,v in wallets.items())

    new_wallets_prefix = []
    new_wallets = []
    requests_output = []


    # Was Any Crypto Spent By These Wallets Deposited into Change Wallets?
    print("Searching For Change...")
    for ltc_wallet in tqdm(ltc_wallets):

        wallet_label = '_'.join(wallet_keys[ltc_wallet].split("_")[:-1])

        r = requests.get(f"https://chain.so/api/v2/get_tx_spent/LTC/{ltc_wallet}")
        

        if len(r.json()['data']['txs'])!=0:
            txn_ids = list(set([k['txid'] for k in r.json()['data']['txs']]))
            requests_output.append(r)

            for txn in txn_ids:
                r = requests.get("https://chain.so/api/v2/get_tx/LTC/{}".format(txn))
                if len(r.json()['data']['outputs'])==2:

                    addrs = [j['address'] for j in r.json()['data']['outputs']] 

                    if len(addrs)>0:
                        [new_wallets.append(addr) for addr in addrs]
                        [new_wallets_prefix.append(wallet_label) for addr in addrs]


    if len(new_wallets)>0:

        output = pd.DataFrame([new_wallets_prefix, new_wallets]).T

        output = output[~output[1].isin([j.lower() for j in (ltc_wallets + [k for k in other_party_wallets.values()])])].copy().reset_index(drop=True)
        output.columns = ['prefix', 'wallet']

        # Come up with appropriate wallet labels
        wallets_df = pd.DataFrame.from_dict({0:wallets}).reset_index()
        for wallet_type in output['prefix'].drop_duplicates():
            prefix_max_val = max([int(j.split('_')[-1]) if j.split('_')[-1].isdigit() else 0 for j in wallets_df[wallets_df['index'].str.contains(wallet_type)]['index']]) + 1
            prefix_match = output[output['prefix']==wallet_type]
            output['new_name'] = ''

            for index in prefix_match.index:
                output.loc[index, 'new_name'] = '_'.join([wallet_type, str(prefix_max_val)])
                prefix_max_val = prefix_max_val + 1

        return output.reset_index(drop=True), requests_output

    else:

        return pd.DataFrame(), requests_output


# In[7]:


new_ltc_wallets, raw_ltc_send_txns = LtcFindNewLedgerWallets(ltc_wallets)
raw_ltc_send_txns = [i.json() for i in raw_ltc_send_txns]


# In[8]:


if len(new_ltc_wallets)>0:
        
    if len([i for i in os.listdir() if re.search("SupportingDocuments", i)])==0:
        wallets_file = open("../SupportingDocuments/Walls.txt", "a")
    else:
        wallets_file = open("SupportingDocuments/Walls.txt", "a")

    for index in new_ltc_wallets.index:
        wallets_file.write("{}\n".format(','.join([new_ltc_wallets.loc[index, 'new_name'], new_ltc_wallets.loc[index, 'wallet']])))

    wallets_file.close()

    wallets = GetWallets()
    ltc_wallets, ltc_wallet_names = ExtractWalletInfo("LTC", wallets)
    
    for index in new_ltc_wallets.index:
        start_time = time.time()
        r = requests.get("https://chain.so/api/v2/get_tx_spent/LTC/{}".format(new_ltc_wallets.loc[index, 'wallet']))
        raw_ltc_requests.append(r)
        
        time_diff = time.time() - start_time
        if (time_diff < 11 & index!=0):
            time.sleep(11-time_diff)


# # Now Get All Wallet Transactions

# ### First Do Spent Since Already Available

# In[9]:


sent_txns = pd.DataFrame(raw_ltc_send_txns)[['data']]
sent_txns = pd.concat([i 
                       for i in sent_txns['data'].apply(lambda x: pd.concat([
                           pd.concat([pd.DataFrame.from_dict({index:i}).T for index, i in enumerate(x['txs'])]), 
                           pd.DataFrame([[x['network'], x['address']]]).rename(columns={0:'Currency',1:'Address'})], axis=1)
                                                       )
                      ]).reset_index(drop=True)
sent_txns['TxnType'] = 'Sent'


# ### Received

# In[10]:


received_txns_raw = []
for wallet_address in ltc_wallets:
    received_txns_raw.append(requests.get(f"https://chain.so/api/v2/get_tx_received/LTC/{wallet_address}"))
    time.sleep(1)
    
received_txns_raw = [i.json() for i in received_txns_raw]
received_txns = pd.DataFrame(received_txns_raw)[['data']]


# In[11]:


received_txns = pd.concat([i 
                           for i in received_txns['data'].apply(lambda x: pd.concat([
                               pd.concat([pd.DataFrame.from_dict({index:i}).T for index, i in enumerate(x['txs'])]), 
                               pd.DataFrame([[x['network'], x['address']]]).rename(columns={0:'Currency',1:'Address'})], axis=1)
                                                       )
                      ]).reset_index(drop=True)
received_txns['TxnType'] = 'Received'


# In[12]:


all_txns = pd.concat([sent_txns, received_txns]).reset_index(drop=True)


# ## Transaction Details

# In[13]:


# all_txns['Wallet_Name'] = all_txns['Address'].map(dict((v,k) for k,v in all_listed_wallets.items()))


# In[30]:


txn_details = []
for txn in all_txns['txid'].drop_duplicates().to_list():
    txn_details.append(requests.get("https://chain.so/api/v2/get_tx/LTC/{}".format(txn)))
    time.sleep(1)
    
txn_details = [i.json() for i in txn_details]

col = {0:'Currency',1:'TxHash',2:'Time',3:"TxnType",4:'Fee'}

txn_details = pd.concat([pd.concat([i 
                                    for i in pd.DataFrame(txn_details)['data'].apply(lambda x: pd.concat([
                                        pd.concat([pd.DataFrame.from_dict({index:i}).T for index, i in enumerate(x['inputs'])]), 
                                        pd.DataFrame([[x['network'], x['txid'], x['time'], "Sent", x['network_fee']]]).rename(columns=col)], axis=1)
                                                                                    )
                                   ]).reset_index(drop=True),
                         pd.concat([i 
                                    for i in pd.DataFrame(txn_details)['data'].apply(lambda x: pd.concat([
                                        pd.concat([pd.DataFrame.from_dict({index:i}).T for index, i in enumerate(x['outputs'])]), 
                                        pd.DataFrame([[x['network'], x['txid'], x['time'], "Received", 0] for a in range(len(pd.concat([pd.DataFrame.from_dict({index:i}).T for index, i in enumerate(x['outputs'])])))]).rename(columns=col)], axis=1)
                                                                                    )
                                   ]).reset_index(drop=True)
]).reset_index(drop=True)

txn_details['Wallet_Name'] = txn_details['address'].map(dict((v,k) for k,v in all_listed_wallets.items()))
txn_details = txn_details[txn_details['Wallet_Name'].isna()==False].reset_index(drop=True)
txn_details['Fee'] = [Decimal(i) for i in txn_details['Fee']]


# ### Determine The Appropriate Wallet Pairs For Each Entry
# If One Send Has Multiple Receivers, Split The Send Instance Into 2

# In[32]:


count_txns_types_per_hash = txn_details[txn_details['TxHash'].isin(txn_details[txn_details['TxnType']=="Sent"]['TxHash'].to_list())].groupby(by=['TxHash', 'TxnType']).agg({'Time':'count'}).reset_index()


# In[33]:


txns_to_split = count_txns_types_per_hash[((count_txns_types_per_hash['TxnType']=="Received") & (count_txns_types_per_hash['Time']>=2))].reset_index(drop=True)

# Split Fees Across Each Wallet Sent to
for x in txns_to_split['TxHash'].copy():
    txn_details.loc[(txn_details['TxHash']==x)
                               & (txn_details['TxnType']=="Received"), 'Fee'] = np.array( [i for i in txn_details[(txn_details['TxHash']==x) 
                & (txn_details['TxnType']=="Received")]['value'].astype(float)])/sum( [i for i in txn_details[(txn_details['TxHash']==x) 
                & (txn_details['TxnType']=="Received")]['value'].astype(float)])*txn_details[(txn_details['TxHash']==x)
                                   & (txn_details['TxnType']=="Sent")]['Fee'].astype(float).mean()


# In[34]:


# When One Wallet Sends to Multiple, Split the Sending in 2 txns

cols_keep = ['address', 'Fee', 'value']
full_append_list = pd.DataFrame()

for x in txns_to_split['TxHash'].copy():

    data_to_fill_in_sent = txn_details.loc[(txn_details['TxHash']==x) & (txn_details['TxnType']=="Sent"), [i for i in txn_details.columns if i not in [j for j in cols_keep if j!='address']]]
    append_data = txn_details.loc[(txn_details['TxHash']==x) & (txn_details['TxnType']=="Received")][['address', 'Fee', 'value']].rename(columns={'address':'Wallet_Pair'})
    for col in data_to_fill_in_sent.columns:
        append_data[col] = data_to_fill_in_sent.loc[0, col]
        
    full_append_list = pd.concat([full_append_list, append_data])
    
txn_details = txn_details.drop(txn_details[(txn_details['TxHash'].isin(txns_to_split['TxHash'].drop_duplicates().to_list()))
                                           & (txn_details['TxnType']=="Sent")].index)

txn_details = pd.concat([txn_details, full_append_list]).reset_index(drop=True)
add_wallet_pairs = txn_details[txn_details["TxnType"]=="Sent"][['TxHash', 'address', 'Wallet_Pair']].rename(columns={'address':'s', 'Wallet_Pair':'address'})
add_wallet_pairs = pd.merge(txn_details[txn_details['Wallet_Pair'].isna()], add_wallet_pairs.copy(), how='left', on=['address', 'TxHash'])[['s']]

txn_details.loc[add_wallet_pairs.index, 'Wallet_Pair'] = add_wallet_pairs['s']


# ### Prepare Data for Final Output

# In[35]:


final_txn_details = pd.concat([
    pd.concat([i for i in txn_details.apply(lambda x: TxnToDf(Txn(x.loc['Wallet_Name'],
                                                                          x.loc['TxHash'],
                                                                          x.loc['Time'],
                                                                          x.loc['Currency'],
                                                                          (-1 if x.loc['TxnType']=="Sent" else 1)*float(x.loc['value']),
                                                                          x.loc['TxnType'],
                                                                          x.loc['Wallet_Pair'])), axis=1)]).sort_values(by='Time').reset_index(drop=True),
    pd.concat([i for i in txn_details.apply(lambda x: TxnToDf(Txn(x.loc['Wallet_Name'],
                                                                          x.loc['TxHash'],
                                                                          x.loc['Time'],
                                                                          x.loc['Currency'],
                                                                          (-1 if x.loc['TxnType']=="Sent" else 0)*float(x.loc['Fee']),
                                                                          '-'.join([x.loc['TxnType'], "Fee"]),
                                                                          x.loc['Wallet_Pair'])), axis=1)]).sort_values(by='Time').reset_index(drop=True)]).reset_index(drop=True)


# ## Reconcile Balances and Txns

# In[81]:


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


# In[84]:


ltc_txns = pd.DataFrame()
ltc_balances = pd.DataFrame()

for wallet_name in ltc_wallet_names:
    txns = final_txn_details[final_txn_details['Wallet']==wallet_name].copy().reset_index(drop=True)
    balances = LTCBalance(wallet_name)
    
    ltc_balances = pd.concat([ltc_balances, balances])
    ltc_txns = pd.concat([ltc_txns, ReconcileTxnsBalance(txns, balances)])

ltc_balances = ltc_balances.reset_index(drop=True) 
ltc_txns = ltc_txns.reset_index(drop=True)  


# In[ ]:


def LTCData(ltc_txns=ltc_txns, ltc_balances = ltc_balances):
    return ltc_balances, ltc_txns

