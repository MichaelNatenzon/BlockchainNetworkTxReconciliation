#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import time
import re
import os

from tqdm import tqdm


# In[63]:


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


# In[64]:


data_source_columns_ordering = DataSourceCols()

btc_base_adjust = 10**8
wallets = GetWallets()
other_party_wallets = GetExternalPartyWallets()

all_listed_wallets = wallets.copy()
all_listed_wallets.update(other_party_wallets)

network = "btc"
globals()[f"{network.lower()}_wallets"], globals()[f"{network.lower()}_wallet_names"] = ExtractWalletInfo(network, wallets)


# ## Pull Data From External Wallets Provided

# ### Handle Ledger / Coinbase Wallet Bitcoin Transactions
# Transactions with ledger and Coinbase wallet are unusual - if you send/receive money with them, an additional wallet will be created to help with the transaction. Cases where this happens must first be accounted for, and these wallets need to be added to the running list of wallets

# In[56]:


def BtcFindNewLedgerWallets(btc_wallets = btc_wallets.copy(), 
                            other_party_wallets = other_party_wallets.copy(),
                            wallets = wallets.copy()):
    
    wallet_keys = dict((v,k) for k,v in wallets.items())
    
    new_wallets_prefix = []
    new_wallets = []
    requests_output = []
    
    
    for btc_wallet in btc_wallets:
        
        wallet_label = '_'.join(wallet_keys[btc_wallet].split("_")[:-1])
        
        start_time = time.time()
        r = requests.get("https://blockchain.info/rawaddr/{}".format(btc_wallet)).json()
        requests_output.append(r)
        
        for txn in r['txs']:
            if ((int(txn['result'])<0) & (int(txn['vout_sz'])==2)):
                addrs = [i['addr'] for i in txn['out'] if i['spent']==False]
                
                if len(addrs)>0:
                    
                    [new_wallets.append(addr) for addr in addrs]
                    [new_wallets_prefix.append(wallet_label) for addr in addrs]
        
        time_diff = time.time() - start_time
        
        if time_diff < 11:
            time.sleep(11-time_diff)
            
    if len(new_wallets)>0:
        
        output = pd.DataFrame([new_wallets_prefix, new_wallets]).T
        
        output = output[~output[1].isin([j.lower() for j in (btc_wallets + [k for k in other_party_wallets.values()])])].copy().reset_index(drop=True)
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


# In[57]:


new_btc_wallets, raw_btc_requests = BtcFindNewLedgerWallets(btc_wallets)


# In[62]:


if len(new_btc_wallets)>0:
        
    if len([i for i in os.listdir() if re.search("SupportingDocuments", i)])==0:
        wallets_file = open("../SupportingDocuments/Walls.txt", "a")
    else:
        wallets_file = open("SupportingDocuments/Walls.txt", "a")

    for index in new_btc_wallets.index:
        wallets_file.write("{}\n".format(','.join([new_btc_wallets.loc[index, 'new_name'], new_btc_wallets.loc[index, 'wallet']])))

    wallets_file.close()

    wallets = GetWallets()
    btc_wallets, btc_wallet_names = ExtractWalletInfo("BTC", wallets)
    
    for index in new_btc_wallets.index:
        start_time = time.time()
        r = requests.get("https://blockchain.info/rawaddr/{}".format(new_btc_wallets.loc[index, 'wallet'])).json()
        raw_btc_requests.append(r)

        time_diff = time.time() - start_time
        if (time_diff < 11 & index!=0):
            time.sleep(11-time_diff)


# ## Format All Bitcoin On-Chain Transactions

# 1. The issue with coins sent from a coinbase account (NOT from a Coinbase Wallet) to a wallet is that the transaction gets grouped in with other transactions, so the fee (and transaction "in" amount) on the chain is for the entire pool, and not just my transaction.<br />
# 2. Ledger transactions involve two wallets at the same time - so we need to account for this in our processing

# To account for this, I will split transactions into:
# 1. 1 Wallet to 1 Wallet Transactions
# 2. 4+ Wallets to 1 Wallet Transactions (and vise-versa) - A Pool of wallets like from coinbase
# 3. Multi Wallet to Multi Wallet Transactions

# Create logic to extract data from 1-1 or 1-many wallet transactions

# In[ ]:


def BtcTxnDetails_Basic(txn_data, wallet_name, wallets = wallets):
    
    qtynet = float(txn_data["result"]/btc_base_adjust)
    qtyout = float([i if txn_data['vout_sz']<=1 else 0 for i in [int(txn_data['inputs'][0]['prev_out']['value'])]][0])/btc_base_adjust
    qtyin = float([int(txn_data["result"]) if i>0 else int(txn_data['out'][0]['value']) for i in [int(txn_data["result"])]][0])/btc_base_adjust
    qtyfee = float([int(txn_data['inputs'][0]['prev_out']['value'])-[int(txn_data["result"]) if i>0 else int(txn_data['out'][0]['value']) for i in [int(txn_data["result"])]][0]
                 if txn_data['vout_sz']<=1 else 0 for i in [int(txn_data['inputs'][0]['prev_out']['value'])]][0])/btc_base_adjust
    
    coin_price = float(DateCoinGecko("bitcoin", txn_data["time"]))
    
    transaction = {
        "Name" : wallet_name,
        "Time" : txn_data["time"],
        "Type" : ["Sent" if i<0 else "Received" for i in [int(txn_data["result"])]][0],
        "DollarNet" : qtynet*coin_price,
        "QtyNet" : qtynet,
        "Currency" : "BTC",
        "QtyOut" : qtyout,
        "QtyFee" : qtyfee,
        "QtyIn" : qtyin,
        "DollarsOut" : qtyout*coin_price,
        "DollarsFee" : qtyfee*coin_price,
        "DollarsIn" : qtyin*coin_price,
        "CoinPrice" : coin_price,
        "Receiver" : [wallets[wallet_name] if i>0 else txn_data['out'][0]['addr'] for i in [int(txn_data["result"])]][0],
        "Sender" : txn_data['inputs'][0]['prev_out']['addr'],
        "TxnHash" : txn_data["hash"]
    }
    
    return pd.DataFrame.from_dict({0:transaction}).T[list(transaction.keys())]


# In[ ]:


def BtcTxnDetails_MultiWallet(txn_details):

    txn_out = pd.DataFrame([[txn_inputs['prev_out']['addr'], txn_inputs['prev_out']['value']] for txn_inputs in txn_details['inputs']],
                           columns = ["Wallet", "Out"])

    txn_out["FeePortion"] = txn_out["Out"].apply(lambda x: x/(txn_out["Out"].sum())*txn_details['fee'])
    txn_out.reset_index(drop=True, inplace=True)

    txn_in = pd.DataFrame([[txn_outputs['addr'], txn_outputs['value']] for txn_outputs in txn_details['out']], 
                          columns=["Wallet", "In"])
    txn_in.reset_index(drop=True, inplace=True)

    txn_in["Proportion"] = txn_in["In"].apply(lambda x: x/(txn_in["In"].sum()))

    new_results = pd.concat([
        pd.concat([txn_out.loc[row_index, "Wallet"]+pd.Series(["" for i in txn_in["Proportion"]]) for row_index in txn_out.index]),
        pd.concat([txn_out.loc[row_index, "Out"]*txn_in["Proportion"] for row_index in txn_out.index]),
        pd.concat([txn_out.loc[row_index, "FeePortion"]*txn_in["Proportion"] for row_index in txn_out.index])], axis=1)

    new_results = new_results.join(pd.concat([txn_in["Wallet"] for i in range(len(txn_in["Proportion"]))]), how='outer')

    new_results.columns = ["Sender", "QtyOut", "QtyFee", "Receiver"]
    new_results = new_results.drop_duplicates()
    new_results.reset_index(drop=True, inplace=True)

    new_results.loc[:, "QtyOut"] = new_results["QtyOut"].copy()/btc_base_adjust
    new_results.loc[:, "QtyFee"] = new_results["QtyFee"].copy()/btc_base_adjust

    new_results["QtyIn"] = new_results["QtyOut"] - new_results["QtyFee"]
    new_results["QtyNet"] = new_results["QtyIn"] if int(txn_details['result'])>0 else -1*new_results["QtyOut"]
    new_results["Currency"] = "BTC"

    new_results["Time"] = txn_details['time']
    new_results["Type"] = "Received" if txn_details['result']>0 else 'Sent'
    new_results["CoinPrice"] = float(DateCoinGecko("bitcoin", txn_details['time']))
    new_results["TxnHash"] = txn_details['hash']

    new_results["DollarNet"] = new_results["QtyNet"]*new_results["CoinPrice"]
    new_results["DollarsOut"] = new_results["QtyOut"]*new_results["CoinPrice"]
    new_results["DollarsFee"] = new_results["QtyFee"]*new_results["CoinPrice"]
    new_results["DollarsIn"] = new_results["QtyIn"]*new_results["CoinPrice"]

    new_results["Name"] = [GetDictKey(all_listed_wallets, new_results.loc[row_index, "Receiver"]) 
                           if new_results.loc[row_index, "Type"]=="Received" 
                           else GetDictKey(all_listed_wallets, new_results.loc[row_index, "Sender"])
                           for row_index in new_results.index] 

    new_results = new_results[data_source_columns_ordering]
    
    return new_results.drop_duplicates()


# In[ ]:


all_btc_wallet_txns = pd.DataFrame()
for wallet_details in tqdm(raw_btc_requests):
    wallet_transactions = wallet_details['txs']
    wallet_name = GetDictKey(all_listed_wallets, wallet_details['address'])

    simple_tnx_indicies = [txn_index for txn_index, i in enumerate(wallet_transactions) if (i['vin_sz']==1 and i['vout_sz']==1)]
    big_pool_txn_indicies = [txn_index for txn_index, i in enumerate(wallet_transactions) if ((i['vin_sz']>3 
                                                                                               and i['vout_sz']==1)
                                                                                              or (i['vout_sz']>3
                                                                                                  and i['vin_sz']==1))]
    multi_wallet_txns = [i for i in range(len(wallet_transactions)) if i not in (simple_tnx_indicies + big_pool_txn_indicies)]
    
    if len(simple_tnx_indicies+big_pool_txn_indicies)>0:
        t = pd.concat([BtcTxnDetails_Basic(wallet_transactions[i], wallet_name) for i in (simple_tnx_indicies+big_pool_txn_indicies)])
    else:
        t = pd.DataFrame()
        
    if len(multi_wallet_txns)>0:
        output = pd.concat([t, pd.concat([BtcTxnDetails_MultiWallet(wallet_transactions[i]) for i in multi_wallet_txns])[data_source_columns_ordering]], sort=True)
    else:
        output = t
    
    all_btc_wallet_txns = pd.concat([all_btc_wallet_txns, output], sort=True)
    
all_btc_wallet_txns.reset_index(drop=True, inplace=True)
all_btc_wallet_txns = all_btc_wallet_txns.dropna(how='all')


# In[ ]:


btc_txns_df = all_btc_wallet_txns[data_source_columns_ordering].reset_index(drop=True)
btc_txns_df = btc_txns_df.drop_duplicates()
btc_txns_df.reset_index(drop=True, inplace=True)


# In[ ]:


btc_txns_df.loc[btc_txns_df[btc_txns_df["Type"]=="Received"].index, "QtyFee"] = 0
btc_txns_df.loc[btc_txns_df[btc_txns_df["Type"]=="Received"].index, "DollarsFee"] = 0


# In[ ]:


btc_txns_df = btc_txns_df[(btc_txns_df["Receiver"].isin(list(wallets.values()))) | (btc_txns_df["Sender"].isin(list(wallets.values())))].reset_index(drop=True)


# In[ ]:


btc_txns_df = pd.concat([
    pd.concat([i for i in btc_txns_df.apply(lambda x: TxnToDf(Txn(x.loc['Name'],
                                                                      x.loc['TxnHash'],
                                                                      x.loc['Time'],
                                                                      x.loc['Currency'],
                                                                      (-1 if x.loc['Type']=="Sent" else 1)*float(x.loc['QtyIn']),
                                                                      x.loc['Type'],
                                                                      (x.loc['Receiver'] if x.loc['Type']=="Sent" else x.loc['Sender']))), axis=1)]),
    pd.concat([i for i in btc_txns_df.apply(lambda x: TxnToDf(Txn(x.loc['Name'],
                                                                  x.loc['TxnHash'],
                                                                  x.loc['Time'],
                                                                  x.loc['Currency'],
                                                                  -1*float(x.loc['QtyFee']),
                                                                  ("Send-Fee" if x.loc['Type']=="Sent" else "Receive-Fee"),
                                                                  (x.loc['Receiver'] if x.loc['Type']=="Sent" else x.loc['Sender']))), axis=1)])]).reset_index(drop=True)


# In[ ]:


def BitcoinData(btc_txns_df=btc_txns_df):
    return btc_txns_df


# In[ ]:




