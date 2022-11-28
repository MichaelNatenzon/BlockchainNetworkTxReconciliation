#!/usr/bin/env python
# coding: utf-8

# In[64]:


import pandas as pd
import requests
import time
import json
import re

import statistics as stats
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime


# In[65]:


try:
    from Classes.ObjectClasses import Txn
    import Auth.AuthTokens as auth
    from Price import DateCoinGecko
    from GeneralFunctions import (GetWallets, ExtractWalletInfo, TxnToDf, ExtractInnerDictAsCol)
except:
    from Network_Integration.Classes.ObjectClasses import Txn
    import Network_Integration.Auth.AuthTokens as auth
    from Network_Integration.Price import DateCoinGecko
    from Network_Integration.GeneralFunctions import (GetWallets, ExtractWalletInfo, TxnToDf, ExtractInnerDictAsCol)


# In[66]:


eth_base_adjust = 10**18
wallets = GetWallets()

for network in ['eth', 'bsc']:
    globals()[f"{network.lower()}_wallets"], globals()[f"{network.lower()}_wallet_names"] = ExtractWalletInfo(network, wallets)


# ## Pull Data From Ethereum / Binance Smart Chain Network

# In[67]:


def EthBncWalletTransactions(wallet_name, 
                             api_key = auth.Etherscan(), 
                             bscscan_api_key = auth.Bscscan(), 
                             wallets = wallets):
    
    wallet = wallets[wallet_name]
    
    all_wallet_transactions = """
    https://api.{}/api?module=account&action=txlist
    &address={}&startblock=0&endblock=99999999&sort=asc
    &apikey={}""".format("bscscan.com" 
                         if re.search("^bsc", wallet_name) 
                         else "etherscan.io",
                         wallet, 
                         bscscan_api_key
                         if re.search("^bsc", wallet_name) 
                         else api_key)
    
    r = requests.get(re.sub("\\n| ", "", all_wallet_transactions))
    
    if r.json()['message']!="No transactions found":
    
        df = pd.concat([pd.DataFrame.from_dict({wallet: i}).T for index, i in enumerate(r.json()["result"])]).sort_values(by="timeStamp", ascending=True)
        df.reset_index(inplace=True)

        df.columns = [i if i!='index' else 'wallet' for i in df.columns]

        output = pd.DataFrame()

        for index in df.index:

            isError = int(df.loc[index, "isError"])
            fee = int(df.loc[index, "gasPrice"])/(10**18)*int(df.loc[index, "gasUsed"])*(10**18)
            txn_type = ["Received" if df.loc[index, "wallet"].upper()==df.loc[index, "to"].upper() else "Sent"][0]
            amount_in = int(df.loc[index, "value"])
            result = [int(df.loc[index, "value"]) 
                            if txn_type=="Received" 
                            else -1*(int(df.loc[index, "value"])+fee)][0]

            amount_out = int(df.loc[index, "value"])+fee


            transaction = {
                "Name" : wallet_name,
                "Time" : df.loc[index, "timeStamp"],
                "Result" : result if isError==0 else -1*fee,
                "Type" : txn_type if isError==0 else "Error",
                "TxnHash" : df.loc[index, "hash"],
                "Sender" : df.loc[index, "from"],
                "Receiver" : df.loc[index, "to"],
                "AmountOut" : amount_out if isError==0 else fee,
                "AmountIn" : amount_in if isError==0 else 0,
                "Fee" : fee
            }

            output = pd.concat([output, pd.DataFrame.from_dict({0:transaction}).T[list(transaction.keys())]])
    
    else:
        output = pd.DataFrame()
    
    return output.reset_index(drop=True)


# #### Ethereum Network
# Send / Receive Eth + Fees

# In[68]:


eth_txns_df = pd.DataFrame()
for name in eth_wallet_names:
    eth_txns_df = pd.concat([eth_txns_df, EthBncWalletTransactions(name)])

eth_txns_df.reset_index(inplace=True, drop=True)
time.sleep(5)


# In[100]:


internal_eth_txns = pd.DataFrame()
for wallet_name in eth_wallet_names:
    wallet = wallets[wallet_name]

    r = requests.get("https://api.etherscan.io/api?module=account&action=txlistinternal&address={}&startblock=0&endblock=999999999&sort=asc&apikey={}".format(wallet, auth.Etherscan()))
    
    if len(r.json()["result"])>0:

        internal_txn = pd.concat([pd.DataFrame.from_dict({wallet: i}).T for index, i in enumerate(r.json()["result"])]).sort_values(by="timeStamp", ascending=True)
        internal_txn['Name'] = wallet_name
        internal_eth_txns = pd.concat([internal_eth_txns, internal_txn])

internal_eth_txns.reset_index(inplace=True)


# In[106]:


# Remove the send transactions in bsc_txns_df that are also in internal_eth_txns
# This is because the send transactions in eth_txns_df are txn fees, which will be accounted for
# When getting ERC20 in a few cells below
eth_txns_df = eth_txns_df[~eth_txns_df["TxnHash"].isin(internal_eth_txns[["hash"]].drop_duplicates()["hash"])].reset_index(drop=True)


# In[107]:


internal_eth_txns = internal_eth_txns[["Name", "hash", "to", "timeStamp", "value", "from"]].copy()
internal_eth_txns.columns = ["Name", "TxnHash", "Receiver", "Time", "AmountIn", "Sender"]
internal_eth_txns["Result"] = internal_eth_txns["AmountIn"].astype(float)
internal_eth_txns["Type"] = "Received"
internal_eth_txns["AmountOut"] = internal_eth_txns["Result"]
internal_eth_txns["Fee"] = float(0)

internal_eth_txns = internal_eth_txns[eth_txns_df.columns]


# In[ ]:


eth_txns_df = pd.concat([eth_txns_df, internal_eth_txns]).reset_index(drop=True)


# #### Binance Smart Chain Network
# Send BSC + Send Fees (Still need Receive BSC Transactions)

# In[69]:


bsc_txns_df = pd.DataFrame()
for name in bsc_wallet_names:
    try:
        result = EthBncWalletTransactions(name)
        bsc_txns_df = pd.concat([bsc_txns_df, result]).reset_index(drop=True)
    except:
        print(f"ERROR: Retrieving {name}")


# Receive BSC Transactions

# In[70]:


internal_bsc_txns = pd.DataFrame()
for wallet_name in bsc_wallet_names:
    wallet = wallets[wallet_name]

    r = requests.get("https://api.bscscan.com/api?module=account&action=txlistinternal&address={}&startblock=0&endblock=999999999&sort=asc&apikey={}".format(wallet, auth.Bscscan()))
    internal_txn = pd.concat([pd.DataFrame.from_dict({wallet: i}).T for index, i in enumerate(r.json()["result"])]).sort_values(by="timeStamp", ascending=True)
    internal_txn['Name'] = wallet_name
    internal_bsc_txns = pd.concat([internal_bsc_txns, internal_txn])
    
internal_bsc_txns.reset_index(inplace=True)


# In[71]:


# Remove the send transactions in bsc_txns_df that are also in internal_bsc_txns
# This is because the send transactions in bsc_txns_df are txn fees, which will be accounted for
# When getting BEP20 in a few cells below
bsc_txns_df = bsc_txns_df[~bsc_txns_df["TxnHash"].isin(internal_bsc_txns[["hash"]].drop_duplicates()["hash"])].reset_index(drop=True)


# Prep Internal BSC (receiving BSC tokens from exchange sale) to Concat with bsc_txns_df

# In[72]:


internal_bsc_txns = internal_bsc_txns[["Name", "hash", "to", "timeStamp", "value", "from"]].copy()
internal_bsc_txns.columns = ["Name", "TxnHash", "Receiver", "Time", "AmountIn", "Sender"]
internal_bsc_txns["Result"] = internal_bsc_txns["AmountIn"].astype(float)
internal_bsc_txns["Type"] = "Received"
internal_bsc_txns["AmountOut"] = internal_bsc_txns["Result"]
internal_bsc_txns["Fee"] = float(0)

internal_bsc_txns = internal_bsc_txns[bsc_txns_df.columns]


# In[73]:


bsc_txns_df = pd.concat([bsc_txns_df, internal_bsc_txns]).reset_index(drop=True)


# ### Apply some additional formatting to prep for big merge with all transactions

# In[74]:


for col in ["Result", "AmountOut", "AmountIn", "Fee"]:
    eth_txns_df.loc[:, col] = eth_txns_df.loc[:, col].astype(float)/eth_base_adjust
    bsc_txns_df.loc[:, col] = bsc_txns_df.loc[:, col].astype(float)/eth_base_adjust
    
eth_txns_df.loc[eth_txns_df[eth_txns_df["Type"]=="Received"].index, "Fee"] = 0


# In[75]:


eth_bsc_txn_df = pd.concat([
    pd.concat([i for i in eth_txns_df.apply(lambda x: TxnToDf(Txn(x.loc['Name'],
                                                                      x.loc['TxnHash'],
                                                                      x.loc['Time'],
                                                                      "ETH",
                                                                      (1 if x.loc['Type']=="Received" else -1)*float(x.loc['AmountIn']),
                                                                      x.loc['Type'],
                                                                      (x.loc['Receiver'] if x.loc['Type']=="Sent" else x.loc['Sender']))), axis=1)]),
    pd.concat([i for i in eth_txns_df.apply(lambda x: TxnToDf(Txn(x.loc['Name'],
                                                                          x.loc['TxnHash'],
                                                                          x.loc['Time'],
                                                                          "ETH",
                                                                          -1*float(x.loc['Fee']),
                                                                          x.loc['Type']+"-Fee",
                                                                          (x.loc['Receiver'] if x.loc['Type']=="Sent" else x.loc['Sender']))), axis=1)]),
    pd.concat([i for i in bsc_txns_df.apply(lambda x: TxnToDf(Txn(x.loc['Name'],
                                                                          x.loc['TxnHash'],
                                                                          x.loc['Time'],
                                                                          "BSC",
                                                                          (1 if x.loc['Type']=="Received" else -1)*float(x.loc['AmountIn']),
                                                                          x.loc['Type'],
                                                                          (x.loc['Receiver'] if x.loc['Type']=="Sent" else x.loc['Sender']))), axis=1)]),
    pd.concat([i for i in bsc_txns_df.apply(lambda x: TxnToDf(Txn(x.loc['Name'],
                                                                          x.loc['TxnHash'],
                                                                          x.loc['Time'],
                                                                          "BSC",
                                                                          -1*float(x.loc['Fee']),
                                                                          x.loc['Type']+"-Fee",
                                                                          (x.loc['Receiver'] if x.loc['Type']=="Sent" else x.loc['Sender']))), axis=1)])]).reset_index(drop=True)

eth_bsc_txn_df["TxnType"] = eth_bsc_txn_df["TxnType"].str.replace("Send-Fee", "Send-Fee", regex=True)
eth_bsc_txn_df["TxnType"] = eth_bsc_txn_df["TxnType"].str.replace("Received-Fee", "Receive-Fee", regex=True)


# #### Get side (Pegged) Token Transactions

# In[76]:


def EthBnc20Tokens(wallet_name, 
                   api_key = auth.Etherscan(),
                   bscscan_api_key = auth.Bscscan()):
    
    wallet = wallets[wallet_name]

    get_erc20_tokens = """
    https://api.{}/api?module=account&action=tokentx
    &address={}
    &startblock=0&endblock=999999999&sort=asc
    &apikey={}
    """.format("bscscan.com" 
                         if re.search("^bsc", wallet_name) 
                         else "etherscan.io",
                         wallet, 
                         bscscan_api_key
                         if re.search("^bsc", wallet_name) 
                         else api_key)
    
    r = requests.get(re.sub("\\n| ", "", get_erc20_tokens))
    
    try:
        df = pd.concat([pd.DataFrame.from_dict({wallet: i}).T for index, i in enumerate(r.json()["result"])]).sort_values(by="timeStamp", ascending=True)
        df.reset_index(inplace=True)
    except:
        return
    
    return df


# In[77]:


on_chain_eth_bnc_tokens = pd.DataFrame()

for wallet in list(set(eth_bsc_txn_df["Wallet"])):
    try:
        output = EthBnc20Tokens(wallet)
        output['wallet'] = wallet
        on_chain_eth_bnc_tokens = pd.concat([on_chain_eth_bnc_tokens, output])
    except:
        pass
    
on_chain_eth_bnc_tokens = on_chain_eth_bnc_tokens.reset_index(drop=True)


# In[78]:


side_token_decimal_places = on_chain_eth_bnc_tokens[['wallet', 'tokenName', 'tokenSymbol', 'tokenDecimal']].drop_duplicates()


# In[79]:


contract_addresses = on_chain_eth_bnc_tokens[['tokenName', 'tokenSymbol', 'contractAddress', 'hash']].drop_duplicates().copy()


# #### Label Tokens with Identical Symbols But Different Names
# One or all may be fraudulent

# In[80]:


piggyback_token_fraud = on_chain_eth_bnc_tokens[['tokenSymbol', 'tokenName']].drop_duplicates().groupby("tokenSymbol").agg({'tokenName' : 'count'}).reset_index()
piggyback_token_fraud = piggyback_token_fraud[piggyback_token_fraud['tokenName']>1].reset_index(drop=True).rename(columns={'tokenName':'tokenName_count'})


# #### The df above (piggyback_token_fraud) will be used later on to remove fraudulent tokens

# In[81]:


on_chain_eth_bnc_tokens["QtyFee"] = [int(on_chain_eth_bnc_tokens.loc[index, "gasPrice"])/(10**18)*int(on_chain_eth_bnc_tokens.loc[index, "gasUsed"]) for index in on_chain_eth_bnc_tokens.index]
on_chain_eth_bnc_tokens["Txntype"] = ["Received" if wallets[on_chain_eth_bnc_tokens.loc[index, "wallet"]].upper()==on_chain_eth_bnc_tokens.loc[index, "to"].upper() else "Sent" for index in on_chain_eth_bnc_tokens.index]
on_chain_eth_bnc_tokens["QtyIn"] = [int(on_chain_eth_bnc_tokens.loc[index, "value"]) / (10**int(on_chain_eth_bnc_tokens.loc[index, "tokenDecimal"])) for index in on_chain_eth_bnc_tokens.index]
on_chain_eth_bnc_tokens["QtyFee"] = [0 if on_chain_eth_bnc_tokens.loc[index, "Txntype"]=="Received" else on_chain_eth_bnc_tokens.loc[index, "QtyFee"] for index in on_chain_eth_bnc_tokens.index]


# In[82]:


on_chain_eth_bnc_tokens = pd.concat([
    pd.concat([i for i in on_chain_eth_bnc_tokens.apply(lambda x: TxnToDf(Txn(x.loc['wallet'],
                                                                          x.loc['hash'],
                                                                          x.loc['timeStamp'],
                                                                          x.loc['tokenSymbol'],
                                                                          (-1 if x.loc['Txntype']=="Sent" else 1) * x.loc['QtyIn'],
                                                                          x.loc['Txntype'],
                                                                          (x.loc['to'] if x.loc['Txntype']=="Sent" else x.loc['from']))), axis=1)]),
    pd.concat([i for i in on_chain_eth_bnc_tokens.apply(lambda x: TxnToDf(Txn(x.loc['wallet'],
                                                                          x.loc['hash'],
                                                                          x.loc['timeStamp'],
                                                                          x.loc['wallet'][:3].upper(),
                                                                          -1 * x.loc['QtyFee'],
                                                                          x.loc['Txntype'] + "-Fee",
                                                                          (x.loc['to'] if x.loc['Txntype']=="Sent" else x.loc['from']))), axis=1)])]).reset_index(drop=True)

on_chain_eth_bnc_tokens["TxnType"] = on_chain_eth_bnc_tokens["TxnType"].str.replace("Send-Fee", "Send-Fee", regex=True)
on_chain_eth_bnc_tokens["TxnType"] = on_chain_eth_bnc_tokens["TxnType"].str.replace("Received-Fee", "Receive-Fee", regex=True)


# ### For Each Txn, Need to Make Sure Fee Is Only Counted Once

# In[83]:


for tx_hash in on_chain_eth_bnc_tokens[on_chain_eth_bnc_tokens['TxnType']=='Sent-Fee']['TxnHash'].value_counts().loc[lambda x: x>1].index:
    relevant_indicies = list(on_chain_eth_bnc_tokens[(on_chain_eth_bnc_tokens['TxnHash']==tx_hash) 
                                                     & (on_chain_eth_bnc_tokens['TxnType']=='Sent-Fee')].index)
    on_chain_eth_bnc_tokens.loc[relevant_indicies, 'QtyNet'] = stats.mean(on_chain_eth_bnc_tokens.loc[relevant_indicies]['QtyNet'])/len(relevant_indicies)


# # Combine BEP20 / ERC20 Tokens
# <b>eth_bsc_txn_df</b> and <b>on_chain_eth_bnc_tokens</b> easily match Eth transactions to ERC-20 Tokens.

# In[84]:


eth_bsc_txn_df = pd.concat([eth_bsc_txn_df, on_chain_eth_bnc_tokens]).reset_index(drop=True)


# ### Now Need to Add BNB Transactions (And Pegs)

# In[85]:


all_bnb_txns = pd.DataFrame()
bnb_wallets_for_balances = [j for j in [i for i in wallets.keys() if re.search("bnb", i)] if not re.search("binance", j)]

for bnb_wallet in bnb_wallets_for_balances:
    wallet_address = wallets[bnb_wallet]
    page_num = 1

    wallet_txns = json.loads(requests.get("https://explorer.binance.org/api/v1/txs?page={}&rows=100&address={}&?format=json".format(page_num, wallet_address)).content)
    txn_count = int(wallet_txns['txNums'])

    if txn_count<101:
        all_bnb_txns = pd.DataFrame.from_dict(wallet_txns['txArray'])
        all_bnb_txns["Wallet"] = bnb_wallet

    else:
        total_pages = math.ceil(txn_count/100)

        for page_num in range(2, total_pages+1):
            wallet_txns = json.loads(requests.get("https://explorer.binance.org/api/v1/txs?page={}&rows=100&address={}&?format=json".format(page_num, wallet_address)).content)
            wallet_txns["Wallet"] = bnb_wallet
            
            all_bnb_txns = pd.concat([all_bnb_txns, pd.DataFrame.from_dict(wallet_txns['txArray'])])


# In[86]:


all_bnb_txns = all_bnb_txns[['Wallet', 'txHash', 'value', 'txFee', 'txAsset', 'txType', 'data', 'fromAddr', 'memo', 'timeStamp', 'toAddr', 'txQuoteAsset']]


# ### Split BNB Transactions based on Category (to SimplifyTxns)

# In[87]:


# Sent to BNB_Wallets
bnb_to_ownwallet = all_bnb_txns[all_bnb_txns["toAddr"].isin([wallets[wallet] for wallet in bnb_wallets_for_balances])]

bnb_to_ownwallet = bnb_to_ownwallet[["Wallet", "txHash", "timeStamp", "txAsset", "value", "fromAddr"]]
bnb_to_ownwallet.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "WalletPair"]
bnb_to_ownwallet.loc[:, "QtyNet"] = bnb_to_ownwallet.loc[:, "QtyNet"].copy()
bnb_to_ownwallet.loc[:,"TxnType"] = "Received"

bnb_to_ownwallet = bnb_to_ownwallet.reset_index(drop=True)


# In[88]:


# Send from BNB_Wallets
bnb_to_binance = all_bnb_txns[all_bnb_txns["toAddr"].isin([wallets[wallet] for wallet in [i for i in wallets.keys() if re.search("bnb_binance", i)]])]
t1 = bnb_to_binance[["Wallet", "txHash", "timeStamp", "txAsset", "value", "toAddr"]].copy()
t1.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "WalletPair"]
t1.loc[:, "QtyNet"] = -1 * t1.loc[:, "QtyNet"].copy()
t1.loc[:, "TxnType"] = "Sent"

t2 = bnb_to_binance[["Wallet", "txHash", "timeStamp", "txAsset", "txFee", "toAddr"]].copy()
t2.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "WalletPair"]
t2.loc[:, "QtyNet"] = -1 * t2.loc[:, "QtyNet"].copy()
t2.loc[:, "TxnType"] = "Send-Fee"

bnb_to_binance = pd.concat([t1, t2])


# #### Now Handle On/Off Chain Swaps

# In[89]:


bnb_other = all_bnb_txns[~((all_bnb_txns["toAddr"].isin([wallets[wallet] for wallet in bnb_wallets_for_balances])) 
                           | (all_bnb_txns["toAddr"].isin([wallets[wallet] for wallet in [i for i in wallets.keys() if re.search("bnb_binance", i)]])))]

bnb_stake = bnb_other[bnb_other["txType"].str.lower().str.contains("sidechain")]
bnb_other = bnb_other[~bnb_other["txType"].str.lower().str.contains("sidechain")]
bnb_swap = bnb_other[bnb_other["txType"].str.lower().str.contains("new_order")]
bnb_offchain_swap = bnb_other[~bnb_other["txType"].str.lower().str.contains("new_order")]


# In[90]:


cross_chain_txn_bridge = []
for txn_hash in list(set(bnb_offchain_swap["txHash"])):
    t = requests.get("https://explorer.binance.org/tx/{}".format(txn_hash))
    sidechain_hash = re.sub("\"", "", re.search("crossChainTxHash\":\"(.*?)\"", str(t.content)).group(0).split(":")[1])
    cross_chain_txn_bridge.append([txn_hash, sidechain_hash])
    
cross_chain_txn_bridge = pd.DataFrame(cross_chain_txn_bridge, columns=["BNB_TxnHash", "BSC_TxnHash"])


# #### Fill in Some Needed Details for On-Chain Swaps

# In[91]:


bnb_swap = pd.concat([bnb_swap.reset_index(drop=True), 
                      pd.concat([i for i in bnb_swap["data"].apply(lambda x: pd.DataFrame.from_dict({0 : json.loads(x)['orderData']}).T)]).reset_index(drop=True)],
                     axis=1)


# In[92]:


bnb_swap_bnb = bnb_swap[["Wallet", "txHash", "timeStamp", "txAsset", "quantity", "side"]].copy()
bnb_swap_bnb.loc[:, "side"] = ['Bought' if i.lower()=='buy' else 'Sold' for i in bnb_swap_bnb.loc[:, "side"].str.lower().str.title()]
bnb_swap_bnb.loc[:, "quantity"] = [1 if bnb_swap_bnb.loc[i, "side"]=="Bought" else -1 for i in bnb_swap_bnb.index]*bnb_swap_bnb.loc[:, "quantity"].astype(float)
bnb_swap_bnb.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "TxnType"]


# In[93]:


bnb_swap_pair = bnb_swap[["Wallet", "txHash", "timeStamp", "txQuoteAsset", "value", "side"]].copy()
bnb_swap_pair.loc[:, "side"] = ["Sold" if i.lower()=="buy" else "Bought" for i in bnb_swap_pair.loc[:, "side"].copy()]
bnb_swap_pair.loc[:, "value"] = [1 if bnb_swap_pair.loc[i, "side"]=="Bought" else -1 for i in bnb_swap_pair.index]*bnb_swap_pair.loc[:, "value"].astype(float)
bnb_swap_pair.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "TxnType"]


# In[94]:


bnb_swap = pd.concat([bnb_swap_bnb, bnb_swap_pair]).reset_index(drop=True)
bnb_swap.loc[:, "WalletPair"] = "Binance-Exchange"


# #### Now Off-Chain Swaps

# In[95]:


bnb_offchain_swap = pd.merge(bnb_offchain_swap, 
                             cross_chain_txn_bridge, 
                             left_on="txHash", 
                             right_on="BNB_TxnHash", 
                             how='inner')[["Wallet", "txHash", "timeStamp", "txAsset", "value", "txFee", "BSC_TxnHash"]]


# In[96]:


t1 = bnb_offchain_swap[["Wallet", "txHash", "timeStamp", "txAsset", "value", "BSC_TxnHash"]].copy()
t1.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "WalletPair"]
t1.loc[:, "QtyNet"] = -1 * t1.loc[:, "QtyNet"]
t1.loc[:, "TxnType"] = "Sent-Offchain"

t2 = bnb_offchain_swap[["Wallet", "txHash", "timeStamp", "txAsset", "txFee", "BSC_TxnHash"]].copy()
t2.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "WalletPair"]
t2.loc[:, "QtyNet"] = -1 * t2.loc[:, "QtyNet"]
t2.loc[:, "TxnType"] = "Send-Fee"

bnb_offchain_swap = pd.concat([t1, t2]).reset_index(drop=True)


# #### Now Stake

# In[ ]:


t1 = bnb_stake[["Wallet", "txHash", "timeStamp", "txAsset", "value", "txType"]].copy()
t1.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet", "TxnType"]
t1.loc[:, "QtyNet"] = [t1.loc[i, "QtyNet"] if re.search("undelegate", t1.loc[i, "TxnType"].lower()) else -1*t1.loc[i, "QtyNet"] for i in t1.index]
t1.loc[:, "TxnType"] = ["Received-Stake" if re.search("undelegate", i.lower()) else "Sent-Stake" for i in t1.loc[:, "TxnType"]]



t2 = bnb_stake[["Wallet", "txHash", "timeStamp", "txAsset", "txFee"]].copy()
t2.columns = ["Wallet", "TxnHash", "TimeStamp", "Currency", "QtyNet"]
t2.loc[:, "QtyNet"] = t2.loc[:, "QtyNet"] * -1
t2.loc[:, "TxnType"] = "StakeTxn-Fee"

bnb_stake = pd.concat([t1, t2]).reset_index(drop=True)
bnb_stake.loc[:, "WalletPair"] = "Exchange-Stake"


# In[ ]:


bnb_txn_df = pd.concat([bnb_to_ownwallet, bnb_to_binance, bnb_swap, bnb_offchain_swap, bnb_stake], sort=True).reset_index(drop=True)


# ### Combine Transactions (Midpoint)

# In[ ]:


combined_results = pd.concat([eth_bsc_txn_df, bnb_txn_df.rename(columns={'TimeStamp' : 'Time'})], sort=True)[eth_bsc_txn_df.columns]


# #### Now Need to Add in BNB for all BSC -> BNB

# In[ ]:


bnb_swapped_in = combined_results[(combined_results["Currency"]=="BSC") 
                                  & (combined_results["TxnType"]=="Sent")
                                  & (combined_results["WalletPair"]=="0x0000000000000000000000000000000000001004")].copy()

bnb_swapped_in.loc[:, "QtyNet"] = bnb_swapped_in.loc[:, "QtyNet"].copy() * -1
bnb_swapped_in.loc[:, "Currency"] = "BNB"
bnb_swapped_in.loc[:, "TxnType"] = "Received"


# Now Of the total QtyNet Above, there is also a transaction fee that has been included
# This transaction fee must be added as a separate row
# And removed from (added to the negative) for the QtyNet of the transaction

driver = webdriver.Firefox()
time.sleep(5)

def BnbTxnHashPair(bsc_txn_hash):
    driver.get(f"https://www.bscscan.com/tx/{bsc_txn_hash}")
    html_source_code = driver.execute_script("return document.body.innerHTML;")
    html_soup: BeautifulSoup = BeautifulSoup(html_source_code, 'html.parser')
    
    wallet_pair = re.sub("^.*https://explorer.binance.org/tx/|</.*$|\\\\\'>", "", re.search("Cross Chain Package.*?<\/a>", str(html_soup)).group(0))[:64]
    
    r = requests.get(f"https://explorer.binance.org/tx/{wallet_pair}")
    
    fee = int(re.sub("[A-Za-z]|:|\,", "", re.search("crosschainfee:.*?\,", re.sub("^.*<code|</code.*$|>|<|\\\\n|&quot;|\s", "", str(r.content)).lower()).group(0)))/(10**8)
    
    return wallet_pair, fee

currency_base = []
for index in bnb_swapped_in.index:
    wallet_pair, fee = BnbTxnHashPair(bnb_swapped_in.loc[index, "TxnHash"])
    bnb_swapped_in.loc[index, "WalletPair"] = wallet_pair
    bnb_swapped_in.loc[index, "Fee"] = fee
    currency_base.append([bnb_swapped_in.loc[index, "Currency"], 
                          bnb_swapped_in.loc[index, "Fee"]])

    time.sleep(3)

time.sleep(2)
driver.quit()

for index in bnb_swapped_in.index:
    combined_results.loc[index, "QtyNet"] = combined_results.loc[index, "QtyNet"] + bnb_swapped_in.loc[index, "Fee"]
    
bnb_swapped_in['QtyNet'] = bnb_swapped_in['QtyNet'] - bnb_swapped_in['Fee']

bnb_swapped_in_fees = bnb_swapped_in.copy()
bnb_swapped_in_fees['TxnType'] = "Send-Fee"
bnb_swapped_in_fees['Currency'] = "BSC"
bnb_swapped_in_fees['QtyNet'] = -1* bnb_swapped_in_fees['Fee']
bnb_swapped_in_fees = bnb_swapped_in_fees[[i for i in bnb_swapped_in_fees.columns if i!='Fee']]

bnb_swapped_in = bnb_swapped_in[[i for i in bnb_swapped_in.columns if i!='Fee']]

all_bnb_swapped = pd.concat([bnb_swapped_in, bnb_swapped_in_fees])
all_bnb_swapped['Time'] = all_bnb_swapped['Time']*1000
combined_results = pd.concat([combined_results, all_bnb_swapped]).reset_index(drop=True)


# ### Need to Label Spam Coins
# These will be coins that are only received by the account, and never sent, purchased, or sold

# In[ ]:


non_spam_coins = ['LINK', 'BNB', 'BSC', 'ETH']

potential_spam_coins = combined_results[~(combined_results['Currency'].isin(non_spam_coins))]['Currency'].drop_duplicates().to_list()

potential_spam_txns = combined_results[combined_results['Currency'].isin(potential_spam_coins)]['TxnHash'].drop_duplicates()

potential_spam_txns_count = combined_results[(combined_results['TxnType']!='Receive-Fee') 
                                             & (combined_results['TxnHash'].isin(potential_spam_txns))][['TxnHash', 'Currency']].drop_duplicates().groupby('TxnHash').agg({'Currency' : 'count'}).reset_index()

potential_spam_tmp_table = pd.merge(potential_spam_txns_count,
                                    combined_results[~(combined_results['Currency'].isin(non_spam_coins))][['Currency', 'TxnHash', 'WalletPair']], 
                                    how='left', 
                                    on='TxnHash')

# Account for "Piggyback_token_fraud"
piggyback_fraud_tmp = potential_spam_tmp_table[potential_spam_tmp_table['Currency_y'].isin(piggyback_token_fraud['tokenSymbol'].to_list())][['Currency_x', 'Currency_y', 'WalletPair']].drop_duplicates()


fraud_wallet_pairs = []
for index in piggyback_token_fraud.index:
    fraud_wallet_pairs = fraud_wallet_pairs + piggyback_fraud_tmp[(piggyback_fraud_tmp['Currency_y']==piggyback_token_fraud.loc[index, 'tokenSymbol']) & (piggyback_fraud_tmp['Currency_x']==1)]['WalletPair'].to_list()
    
rename_fraud_wallet_pairs = piggyback_fraud_tmp[piggyback_fraud_tmp['WalletPair'].isin(fraud_wallet_pairs)][['Currency_y', 'WalletPair']].drop_duplicates()
rename_fraud_wallet_pairs['Currency_y'] = rename_fraud_wallet_pairs['Currency_y'] + "_Fraud"
rename_fraud_wallet_pairs.reset_index(inplace=True, drop=True)
rename_fraud_wallet_pairs['Currency_y'] = rename_fraud_wallet_pairs['Currency_y'] + "_" + pd.Series([str(i) for i in rename_fraud_wallet_pairs.index])


potential_spam_tmp_table = pd.merge(potential_spam_tmp_table, rename_fraud_wallet_pairs, how='left', on='WalletPair')
potential_spam_tmp_table['Currency_y_y'] = potential_spam_tmp_table['Currency_y_y'].fillna(potential_spam_tmp_table['Currency_y_x'])


# In[ ]:


token_spam_label = potential_spam_tmp_table[['Currency_y_y', 'Currency_x']].drop_duplicates().sort_values(by='Currency_y_y').groupby('Currency_y_y').sum().reset_index()
token_spam_label.columns = ['Token', 'Suspected_Spam']
token_spam_label['Suspected_Spam'] = ['No' if i>1 else 'Yes' for i in token_spam_label['Suspected_Spam']]


token_spam_label = pd.merge(token_spam_label.copy(), 
                            potential_spam_tmp_table[['Currency_y_x', 'Currency_y_y']].rename(columns={"Currency_y_x" : "NativeSymbol"}).drop_duplicates(),
                            how='left', left_on='Token', right_on='Currency_y_y')[['Token', 'NativeSymbol', 'Suspected_Spam']]


# #### From All Transactions, Label the Fraudulent Ones

# In[ ]:


rename_fraud_wallet_pairs = pd.merge(rename_fraud_wallet_pairs.copy(), token_spam_label[token_spam_label['Suspected_Spam']=='Yes'], how='inner', left_on='Currency_y', right_on='Token')[['Currency_y', 'WalletPair']]


# In[ ]:


combined_results = pd.merge(combined_results.copy(), rename_fraud_wallet_pairs, how='left', on='WalletPair')
combined_results['Currency'] = combined_results['Currency_y'].fillna(combined_results['Currency'])
combined_results = combined_results[[i for i in combined_results.columns if i!='Currency_y']]

combined_results = pd.merge(combined_results.copy(), token_spam_label, how='left', left_on='Currency', right_on='Token')
combined_results['Suspected_Spam'] = combined_results['Suspected_Spam'].fillna('No')
combined_results = combined_results[[i for i in combined_results.columns if i!='Token']]

spam_indicies = combined_results[combined_results['TxnHash'].isin(combined_results[combined_results['Suspected_Spam']=='Yes']['TxnHash'].to_list())].index.copy()
combined_results.loc[spam_indicies, 'Suspected_Spam'] = 'Yes'


# In[ ]:


all_eth_bsc_bnb_transactions = combined_results.reset_index(drop=True).copy()


# ### Finally, re-label "BNB" received to bsc_wallet to be received by the appropriate bnb wallet

# In[ ]:


relabel_bnb_indicies = all_eth_bsc_bnb_transactions[(all_eth_bsc_bnb_transactions['Wallet'].str.startswith("bsc"))
                                                    & (all_eth_bsc_bnb_transactions['Currency'].str.startswith("BNB"))].index


# In[ ]:


def GetBNBTxnWalletPair(txn_hash):
    r = requests.get(f"https://explorer.binance.org/tx/{txn_hash}")
    return [j for j in list(set([re.sub("^.*?\[|\].*$|\\\\n|&quot;|\s|\"", "", i).replace("\\", "") for i in re.findall("receiverAddresses.*?]", str(r.content))])) if j in list(wallets.values())][0]


# In[ ]:


all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'NewWallet'] = all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'WalletPair'].apply(lambda x: GetBNBTxnWalletPair(x)).map({v: k for k, v in wallets.items()})
all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'NewWalletPair'] = all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'Wallet'].map(wallets)

all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'Wallet'] = all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'NewWallet']
all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'TxnHash'] = all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'WalletPair']
all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'WalletPair'] = all_eth_bsc_bnb_transactions.loc[relabel_bnb_indicies, 'NewWalletPair']

all_eth_bsc_bnb_transactions = all_eth_bsc_bnb_transactions[[i for i in all_eth_bsc_bnb_transactions.columns if i not in ['NewWallet', 'NewWalletPair']]].copy()
all_eth_bsc_bnb_transactions['NativeSymbol'] = all_eth_bsc_bnb_transactions['NativeSymbol'].copy().fillna(all_eth_bsc_bnb_transactions['Currency'].copy())


# # Compare to Current Balance
# To find this for each erc20/beb20 in each wallet, first need to find a contract address for each of these tokens

# In[ ]:


bep_twenty_contract_addresses = pd.merge(combined_results[~(combined_results['Currency'].isin(['BSC', 'BNB', 'ETH']))][['Wallet', 'Currency', 'TxnHash', 'Suspected_Spam']], 
                                         contract_addresses[['tokenName', 'tokenSymbol', 'contractAddress', 'hash']], 
                                         how='inner',
                                         left_on='TxnHash', right_on='hash')[['Wallet', 'tokenSymbol', 'tokenName', 'contractAddress', 'Suspected_Spam']].drop_duplicates()

bep_twenty_contract_addresses['Network'] = bep_twenty_contract_addresses['Wallet'].str.split("_").apply(lambda x: x[0]).str.upper()
bep_twenty_contract_addresses['WalletAddress'] = bep_twenty_contract_addresses['Wallet'].map(wallets)


# In[ ]:


def EthBscWalletTokenBalance(network, contract_address, wallet_address, eth_api = auth.Etherscan(), bsc_api = auth.Bscscan()):

    if network.upper() == "ETH":
        r = requests.get(re.sub("\\n|\s", "", """
        https://api.etherscan.io/api   
        ?module=account   
        &action=tokenbalance   
        &contractaddress={}   
        &address={}   
        &tag=latest
        &apikey={}""".format(contract_address, wallet_address, eth_api)))  
        
        return r.json()['result']
        
        
    elif network.upper() == "BSC":
        r = requests.get(re.sub("\\n|\s", "", """https://api.bscscan.com/api
           ?module=account
           &action=tokenbalance
           &contractaddress={}
           &address={}
           &tag=latest
           &apikey={}""".format(contract_address, wallet_address, bsc_api)))
        
        return r.json()['result']
        
    else:
        return f"{network} Not Supported"
    


# In[ ]:


bep_twenty_contract_addresses['Balance'] = bep_twenty_contract_addresses.apply(lambda x: EthBscWalletTokenBalance(x['Network'], x['contractAddress'], x['WalletAddress']), axis=1)
bnb_eth_token_balances = bep_twenty_contract_addresses.copy()
bnb_eth_token_balances = bnb_eth_token_balances.rename(columns={'tokenSymbol' : 'Symbol', 'tokenName' : 'Name', 'contractAddress' : 'Contract'})


# ### Get BNB Token Balances

# In[ ]:


## Get Binance (BNB) Wallet Balances - Excluding The Binance Account (Since not private)
bnb_wallet_balances = pd.DataFrame()
bnb_wallets_for_balances = [i for i in wallets.keys() if (re.search("bnb", i) and not re.search("binance", i))]
for bnb_wallet in bnb_wallets_for_balances:
    bnb_balance = pd.DataFrame.from_dict(requests.get("https://dex.binance.org/api/v1/account/{}".format(wallets[bnb_wallet])).json()['balances'])
    bnb_balance["Name"] = bnb_wallet
    bnb_wallet_balances = pd.concat([bnb_wallet_balances, bnb_balance])
    
bnb_wallet_balances['Balance'] = bnb_wallet_balances['free'].astype(float) + bnb_wallet_balances['frozen'].astype(float) + bnb_wallet_balances['locked'].astype(float)
bnb_wallet_balances.columns = [i.capitalize() for i in bnb_wallet_balances.columns]
bnb_wallet_balances['WalletAddress'] = bnb_wallet_balances['Name'].map(wallets)
bnb_wallet_balances['Suspected_Spam'] = "No"
bnb_wallet_balances = bnb_wallet_balances.rename(columns={'Name' : 'Wallet'})
bnb_wallet_balances['Name'] = "Binance Token"
bnb_wallet_balances['Contract'] = ""


# ### Get BSC Token Balances

# In[ ]:


def BscWalletBalance(wallet_address, bsc_api = auth.Bscscan()):

    r = requests.get(re.sub("\\n|\s", "", """
    https://api.bscscan.com/api  
    ?module=account   
    &action=balance   
    &address={}   
    &apikey={}""").format(wallet_address, bsc_api)).json()['result']
    
    return r


# In[ ]:


bsc_wallet_balances = combined_results[combined_results['Wallet'].str.startswith("bsc")][['Wallet']].drop_duplicates().reset_index(drop=True)
bsc_wallet_balances['WalletAddress'] = bsc_wallet_balances['Wallet'].map(wallets)
bsc_wallet_balances['Balance'] = bsc_wallet_balances['WalletAddress'].apply(lambda x: BscWalletBalance(x))

bsc_wallet_balances['Symbol'] = "BSC"
bsc_wallet_balances['Suspected_Spam'] = "No"
bsc_wallet_balances['Name'] = "Binance Smart Chain"
bsc_wallet_balances['Contract'] = ""


# ### Get ETH Token balance

# In[ ]:


def ETHWalletBalance(wallet_address, eth_api = auth.Etherscan()):

    r = requests.get(re.sub("\\n|\s", "", """
    https://api.etherscan.io/api 
    ?module=account   
    &action=balance   
    &address={}   
    &tag=latest
    &apikey={}""").format(wallet_address, eth_api)).json()['result']
    
    return r


# In[ ]:


eth_wallet_balances = combined_results[combined_results['Wallet'].str.startswith("eth")][['Wallet']].drop_duplicates().reset_index(drop=True)
eth_wallet_balances['WalletAddress'] = eth_wallet_balances['Wallet'].map(wallets)
eth_wallet_balances['Balance'] = eth_wallet_balances['WalletAddress'].apply(lambda x: ETHWalletBalance(x))

eth_wallet_balances['Symbol'] = "ETH"
eth_wallet_balances['Suspected_Spam'] = "No"
eth_wallet_balances['Name'] = "Ethereum Token"
eth_wallet_balances['Contract'] = ""


# In[ ]:


target_columns = ['Wallet', 'WalletAddress', 'Balance', 'Symbol', 'Name', 'Suspected_Spam', 'Contract']


# In[ ]:


all_wallet_balances = pd.concat([bnb_eth_token_balances[target_columns],
                                 bnb_wallet_balances[target_columns],
                                 bsc_wallet_balances[target_columns],
                                 eth_wallet_balances[target_columns]]).reset_index(drop=True)

all_wallet_balances['NewSymbol'] = all_wallet_balances['Contract'].map({v: k for k, v in rename_fraud_wallet_pairs.set_index("Currency_y").to_dict()['WalletPair'].items()} )
all_wallet_balances['NewSymbol'] = all_wallet_balances['NewSymbol'].fillna(all_wallet_balances['Symbol'])
all_wallet_balances = all_wallet_balances[[i for i in all_wallet_balances.columns if i!='Symbol']].rename(columns={"NewSymbol" : "Symbol"}).copy()
all_wallet_balances = all_wallet_balances[[i for i in all_wallet_balances.columns[:3]] + ['Symbol'] + [i for i in all_wallet_balances.columns[3:-1]]].copy()
all_wallet_balances["Balance"] = all_wallet_balances["Balance"].astype(float)


# ### Adjust for Token Decimals

# In[ ]:


all_wallet_balances = pd.merge(all_wallet_balances.copy(), 
                               side_token_decimal_places, 
                               how='left', 
                               left_on=['Wallet', 'Name'], 
                               right_on=['wallet', 'tokenName'])[[i for i in all_wallet_balances.columns] + ['tokenDecimal']]

all_wallet_balances['tokenDecimal'] = all_wallet_balances['Symbol'].map({"ETH-1C9" : 0,  "BNB" : 0, "BSC" : 18, "ETH" : 18 } ).fillna(all_wallet_balances['tokenDecimal']).astype(float)
all_wallet_balances["Balance"] = all_wallet_balances["Balance"]/(10**all_wallet_balances["tokenDecimal"])
all_wallet_balances = all_wallet_balances[[i for i in all_wallet_balances.columns if i!='tokenDecimal']].copy()


# ### Compare Balance to Txn Net

# In[ ]:


txn_net = all_eth_bsc_bnb_transactions.groupby(by=["Wallet", "Currency", "NativeSymbol"]).agg({"QtyNet" : "sum"}).reset_index()


# In[ ]:


reconciled_txn_data = pd.merge(txn_net, all_wallet_balances.drop_duplicates().groupby(by=["Wallet", "Symbol", "Suspected_Spam"]).agg({"Balance" : "sum"}).reset_index(), how='left', left_on=['Wallet', 'Currency'], right_on=['Wallet', 'Symbol'])
reconciled_txn_data['NetUntraceableFeesAndRewards'] = (reconciled_txn_data["Balance"] - reconciled_txn_data["QtyNet"])


# ### For Cases Where the Balance and Txn Don't Match Perfectly, Create An Entry for Gains / Losses

# In[ ]:


add_txns_to_reconcile = reconciled_txn_data[reconciled_txn_data['NetUntraceableFeesAndRewards']!=0].copy()
add_txns_to_reconcile['TxnType'] = ['Unreconcileable Received' if i>0 else 'Unreconcileable Loss' for i in add_txns_to_reconcile['NetUntraceableFeesAndRewards']]
add_txns_to_reconcile = add_txns_to_reconcile[[i for i in add_txns_to_reconcile.columns if i not in ['Symbol', 'QtyNet', 'Balance']]].copy()
add_txns_to_reconcile = add_txns_to_reconcile.rename(columns={'NetUntraceableFeesAndRewards':'QtyNet'})
add_txns_to_reconcile['Time'] = 0
add_txns_to_reconcile['WalletPair'] = " - ".join([datetime.fromtimestamp(int(min([i if i<10000000000 else int(i/1000) for i in all_eth_bsc_bnb_transactions['Time']]))).strftime("%Y-%m-%d"),
                                                  datetime.now().strftime("%Y-%m-%d")])
add_txns_to_reconcile['TxnHash'] = add_txns_to_reconcile['WalletPair'].apply(lambda x: "Unreconcileable: {}".format(x))
add_txns_to_reconcile = add_txns_to_reconcile[['Wallet', 'TxnHash', 'QtyNet', 'Currency', 'TxnType', 'Time', 'WalletPair', 'NativeSymbol', 'Suspected_Spam']].copy()


# In[ ]:


all_eth_bsc_bnb_transactions = pd.concat([all_eth_bsc_bnb_transactions.copy(), add_txns_to_reconcile]).reset_index(drop=True)


# In[ ]:


all_eth_bsc_bnb_transactions['Time'] = [i if i<10000000000 else int(i/1000) for i in all_eth_bsc_bnb_transactions['Time']]


# ### Readjust For Token Base

# In[ ]:


def EthBscBnbData(all_wallet_balances = all_wallet_balances,
                  all_eth_bsc_bnb_transactions = all_eth_bsc_bnb_transactions):
    return all_wallet_balances, all_eth_bsc_bnb_transactions

