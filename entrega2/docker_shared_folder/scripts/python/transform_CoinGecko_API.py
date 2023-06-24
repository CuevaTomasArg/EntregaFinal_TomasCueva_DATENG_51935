import pandas as pd

class Transform():
    def __init__(self, df):
        self.df = df

    def transformation_top(self):
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

        self.df = self.df.loc[:, selected_columns]
    
    def transform_markets_chart(self):
        self.df['timestamp'] = self.df['prices'].str[0]
        self.df['prices'] = self.df['prices'].str[1]
        self.df['market_caps'] = self.df['market_caps'].str[1]
        self.df['total_volumes'] = self.df['total_volumes'].str[1]