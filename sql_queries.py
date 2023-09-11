def to_lowercase_tuple(strings):
    lowercase_strings = [s.lower() for s in strings]
    if len(lowercase_strings) == 1:
        return "('" + lowercase_strings[0] + "')"
    else:
        return tuple(lowercase_strings)

def sql_graph_ethereum(addresses,limit,rank_by):
    
    sql_query = f"""
    WITH 
    tokens_from AS
    ( 
        SELECT  symbol,
                decimals, 
                amount, 
                amount_usd, 
                tx_hash, 
                from_address, 
                to_address, 
                block_timestamp
        FROM ethereum.core.ez_token_transfers
        WHERE lower(from_address) IN {to_lowercase_tuple(addresses)}
    ),
  
    tokens_to AS
    ( 
        SELECT  symbol,
                decimals, 
                amount, 
                amount_usd, 
                tx_hash, 
                from_address, 
                to_address, 
                block_timestamp
        FROM ethereum.core.ez_token_transfers
        WHERE lower(to_address) IN {to_lowercase_tuple(addresses)}
    ),
 
    eth_from AS
    ( 
        SELECT  'ETH' AS symbol,
                18 AS decimals, 
                amount, 
                amount_usd, 
                tx_hash, 
                eth_from_address, 
                eth_to_address, 
                block_timestamp
        FROM ethereum.core.ez_eth_transfers
        WHERE lower(eth_from_address) IN {to_lowercase_tuple(addresses)}
    ),
  
    eth_to AS
    ( 
        SELECT  'ETH' AS symbol,
                18 AS decimals, 
                amount, 
                amount_usd, 
                tx_hash, 
                eth_from_address, 
                eth_to_address, 
                block_timestamp
        FROM ethereum.core.ez_eth_transfers
        WHERE lower(eth_to_address) IN {to_lowercase_tuple(addresses)}
    ),

    tokens AS
    (
      SELECT * FROM tokens_from
      UNION ALL
      SELECT * FROM tokens_to
    ),
  
    eth AS
    (
      SELECT * FROM eth_from
      UNION ALL
      SELECT * FROM eth_to
    )  
  
    SELECT DISTINCT * FROM tokens
    UNION ALL
    SELECT DISTINCT * FROM eth
    ORDER BY {rank_by} DESC NULLS LAST
    LIMIT {limit}
    """

    return sql_query
    
def sql_labels_ethereum(addresses):
    
    sql_query = f"""
    SELECT address, label, label_subtype, label_type
    FROM ethereum.core.dim_labels
    WHERE lower(address) IN {to_lowercase_tuple(addresses)}
    """
    return sql_query
    
def sql_contracts_ethereum(addresses):
    
    sql_query = f"""
    SELECT address, name, symbol as label
    FROM ethereum.core.dim_contracts
    WHERE lower(address) IN {to_lowercase_tuple(addresses)}
    """
    return sql_query

