import pandas as pd
import networkx as nx
import pyvis as pv
import copy
from pyvis.network import Network
from urllib.request import Request, urlopen
from sql_queries import sql_labels_ethereum, sql_graph_ethereum, sql_contracts_ethereum

def grow_df(seed_addresses,
            nogrow_addresses,
            sdk,
            address_label_dict={},
            contracts=[],
            spam_symbols=[], 
            df=pd.DataFrame(),
            drop_spam=True,
            limit_connections='500',
            rank_by='amount_usd',
            stop_at_label=True):
    
    """
    Grows a dataframe from a set of seed addresses and information about a previous dataframe
    (address_label_dict,contracts,df), assumes previous dataframe was fully organized (labelled) already.
    Still works if this is used as the first step (empty address_label_dict,contracts,df)
    
    Args:
        seed_addresses: list of addresses of interest
        nogrow_addresses: list of addresses that we won't query (i.e we aren't interested in finding other transactions related to these addresses)
        sdk: flipside api instance
        address_label_dict: dictionary with addresses as keys and labels as values for any previous state of the graph
        contracts: list of addresses already confirmed to be contract addresses (don't want to continue to grow the graph from these as there are likely too many irrelevant connections)
        df: pandas.DataFrame() object from previous version of the graph
        drop_spam: Bool, if true removes transactions involving spam tokens (not having a token symbol in the api query)
        limit_connections: Limit on number of transaction results returned by address query
        rank_by: With above limi_connections, determines which transactions make the cut
        stop_at_label: Bool, if true adds labelled addressses to nogrow_addresses, e.g. so that once a "Binance" is found, we don't look for all connections to that label
    
    """
    
    # remove any nogrow_addresses we don't want to grow the dataframe (df) from
    seed_addresses = [x for x in seed_addresses if x not in nogrow_addresses]
        
    # run query for all seed address and build a dataframe of transactions
    print(f"Running address query")
    result = sdk.query(sql_graph_ethereum(seed_addresses,limit_connections,rank_by))
    if dict(result)["records"] == None:
        print('Warning: No records found for address(es)')
        address_label_dict_new = address_label_dict
        contracts_new = contracts
        df_new = df
    else:
        # make new dataframe with results
        df_new = pd.json_normalize(result.records)
        # drop spam token transactions with no token symbol (suggested)
        if drop_spam:
            df_new = df_new[~df_new.symbol.isnull()]     
        
        # drop known spam token symbols
        df_new = df_new[~df_new.symbol.isin(spam_symbols)]
        # also drop "zero decimal" tokens as they are likely to be bugged in the api or spam
        df_new = df_new[df_new.decimals!=0]

        # generate list of all unique addresses
        new_addresses = df_new.from_address.to_list() + df_new.to_address.to_list() 
        new_addresses = list(set(new_addresses)) # get unique
        
        # Initiate new empty dict for indexing new labels
        address_label_dict_new = {}
        # look for contracts (dont want to grow the graph from these because too many possible connections)
        print(f"Running contract query")
        result = sdk.query(sql_contracts_ethereum(new_addresses))
        if dict(result)["records"] == None:
            print('No contracts found')
            contracts_new = contracts
        else:
            # make new dataframe with results
            contracts_new = pd.json_normalize(result.records)
            # use labels if any to update address_label_dict_new
            address_label_dict_new.update({row['address']: row['label'] for index, row in contracts_new.iterrows() if row['label'] != None})    
            # add new contracts to list of input contracts
            contracts_new = list(set(contracts_new['address'])) # convert to list
            contracts_new.extend(contracts) # append to input list of contracts

        # run query to get labels
        print(f"Running label query")
        result = sdk.query(sql_labels_ethereum(new_addresses))
        if dict(result)["records"] == None:
            print('No labels found')
        else:
            # make new dataframe with results
            labels_new = pd.json_normalize(result.records)
            # add new labels to dict
            address_label_dict_new.update({row['address']: row['label'] for index, row in labels_new.iterrows()}) # overwrites contract labels if new label (labels from label query are generally better than from contract query)
        
        # shorten unlabelled addresses
        address_label_dict_new.update({x: x[0:3] + x[-3:] for x in new_addresses if x not in list(address_label_dict_new.keys())}) 
        
        # update address_label_dict using previous map
        address_label_dict_new.update(address_label_dict)
    
        # append new df to pre-existing df
        df_new = pd.concat([df,df_new])
        # remove duplicate tx_hash again if any
        df_new = df_new.drop_duplicates()
    
    
    # create dict of labels, and all associated full-addresses (opposite of address_label_dict)
    label_address_dict = {}
    for key, value in address_label_dict_new.items():
        if value not in label_address_dict:
            label_address_dict[value] = [key]
        else:
            label_address_dict[value].append(key)


    # generate new list of seed addresses that would grow the graph one step further if needed
    # exclude previous seed addresses, as we have already grown from there. Exclude contracts, optionally exclude labelled addresses (recommended).
    
    if stop_at_label:
        adds = [add for add, label in address_label_dict_new.items() if label.startswith('0x')]
    else:
        adds = [add for add, label in address_label_dict_new.items()]
    
    seed_addresses = [add for add in adds if add not in seed_addresses]
    seed_addresses = [add for add in seed_addresses if add not in nogrow_addresses]
    

    return seed_addresses, nogrow_addresses, address_label_dict_new, label_address_dict, contracts_new, df_new

    
def draw_graph(df,address_label_dict,label_address_dict,contracts,name):
    
    """
    Creates an html graph visualization for a dataframe of transaction data and wallet labels
    
    args:
        df: pandas.DataFrame() of transactions
        addresss_label_dict: dictionary of each address and its associated label
        label_addresss_dict: dictionary of each label and all associated addresses
        contracts: list of addresses that are known to be contracts
        name: string of html graph filename, e.g "test" -> "test.html"
    """
    
    # color palette
    color_palette = ['#2d728f', '#F5EE9E', '#AB3428', '#F49E4C', '#3b8ea5']
    
    # Enrich df with from/to labels
    df_new = copy.copy(df) # just to avoid inplace operations altering the df outside of the function
    df_new = df_new.reset_index(drop=True) # some indexes will have been repeated, which would cause trouble
    for index, row in df_new.iterrows():
        from_add = row["from_address"]
        to_add = row["to_address"]
        # add from/to labels (so we can group transactions by node labels)
        from_label = address_label_dict[from_add]
        to_label = address_label_dict[to_add]
        df_new.loc[index,"from_label"] = from_label
        df_new.loc[index,"to_label"] = to_label
        # All rows now labelled with from_label/to_label
    
    # now enrich with summary statistics for each edge, vols, net vols, n_transactions,
    for index, row in df_new.iterrows():
        from_label = row["from_label"]
        to_label = row["to_label"]
        df_out = df_new.query(f"from_label=='{from_label}' and to_label=='{to_label}'")["amount_usd"]
        df_in = df_new.query(f"from_label=='{to_label}' and to_label=='{from_label}'")["amount_usd"]
        n_transactions = int(len(df_out) + len(df_in))
        vol_out = df_out.sum(skipna=True)
        vol_in = df_in.sum(skipna=True)
        vol = vol_in + vol_out
        net_vol = vol_out - vol_in
        
        # add volume and label info to df_new
        df_new.loc[index,"usd_net_vol_out"] = net_vol
        df_new.loc[index,"usd_vol"] = vol
        df_new.loc[index,"n_transactions"] = n_transactions
        
    
    # sort (this helps keep the "middle" arrow correct when drawing graph, such that it's parallel to net-flow between nodes)
    #df_new = df_new.sort_values("usd_net_vol_out")
    
    # make graph from transaction df_new (edgelist) 
    G = nx.from_pandas_edgelist(df_new,
                            source='from_label',
                            target = 'to_label',
                            edge_attr = ('symbol','amount','amount_usd',
                                         'tx_hash','block_timestamp','from_address',
                                         'to_address','from_label','to_label',
                                         'n_transactions','usd_vol','usd_net_vol_out'),
                            create_using=nx.MultiDiGraph,
                           )
    
    
    # use this label_address_dict to add these associated addresses to each node as an attribute
    nx.set_node_attributes(G,label_address_dict,"full_addresses")
    
    # PyVis #
    net = Network(height="1500px", 
                  width="100%", 
                  bgcolor=color_palette[0], 
                  font_color='white',
                  select_menu=True,
                  filter_menu=True
                  )
    
    net.repulsion()
    net.from_nx(G)

    vol_usd_max = df_new['usd_vol'].max() # for scaling visual width of edges 
                    
    for node in net.nodes:
        
        node["title"] = "Addresses:\n" + "\n".join(set(node["full_addresses"]))
        if not node["label"].startswith('0x'):
            if any(x in node["full_addresses"] for x in contracts): # contracts shaped as squares
                node["size"] = 15
                node["color"] = color_palette[3]
                node["shape"] = 'square'
            else:
                node["size"] = 30
                node["color"] = color_palette[3]
        else:
            node["color"] = color_palette[2]
            if any(x in node["full_addresses"] for x in contracts):
                node["shape"] = 'square'
                    
    for edge in net.edges:

        # find volume of all edges with these endpoints
        
        out_bigger = edge["usd_net_vol_out"]>=0
        usd_vol = edge['usd_vol']
        usd_net_vol = edge['usd_net_vol_out']
        # edge decoration
        edge["width"] = 1 + 20*usd_vol/vol_usd_max
        edge["weight"] = 1 + 5*usd_vol/vol_usd_max
        if edge["from"].startswith('0x') and edge["to"].startswith("0x"):
            edge["color"] = color_palette[2]
        else:
            edge["color"] = color_palette[3]
        
        
        if out_bigger:
            edge["title"] = f"Volume = ${usd_vol:,.2f}\nNet Volume = ${abs(usd_net_vol):,.2f} ({edge['from']}-->{edge['to']})\nn_transactions = {edge['n_transactions']:.0f}"
        else:
            edge["title"] = f"Volume = ${usd_vol:,.2f}\nNet Volume = ${abs(usd_net_vol):,.2f} ({edge['to']}-->{edge['from']})\nn_transactions = {edge['n_transactions']:.0f}"    
            
    net.show_buttons(('physics','edges'))
    net.save_graph(name +'.html')    
    
    return net, df_new