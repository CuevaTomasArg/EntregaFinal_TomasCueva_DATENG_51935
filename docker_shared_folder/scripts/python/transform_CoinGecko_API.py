import pandas as pd
import numpy as np

class Transform():
    def __init__(self):
        self.array_df = np.array([])
    
    def transformation_top(self,json):
        df = pd.DataFrame(json)
        selected_columns = [
            'id', 
            'symbol', 
            'name', 
            'current_price', 
            'market_cap', 
            'market_cap_rank', 
            'total_volume', 
            'high_24h',
            'low_24h',
            'price_change_24h',
            'price_change_percentage_24h',
            'market_cap_change_24h',
            'market_cap_change_percentage_24h',
            'circulating_supply',
            'ath',
            'ath_change_percentage',
            'ath_date',
            'atl',
            'atl_change_percentage',
            'atl_date',
            'last_updated',
        ]
        df = df.loc[:, selected_columns]
        
        return df
    
    def transform_markets_chart(self,json,cripto):
        df = pd.DataFrame(json)
        df['timestamp'] = df['prices'].str[0]
        df['prices'] = df['prices'].str[1]
        df['market_caps'] = df['market_caps'].str[1]
        df['total_volumes'] = df['total_volumes'].str[1]
        df['cripto'] = cripto
        
        return df
    
    def add_df(self,json):
        df = self.transform_markets_chart(json)
        self.array_df = np.append(self.array_df, df.copy())
    
    def get_markets_chart(self):
        total_markets_chart = pd.concat(self.array_df)
        
        return total_markets_chart