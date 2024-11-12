import pandas as pd
import requests
import polygon

api_key = 'xDzNPfq0pbNuY65_75SrUqaMG1tGuguP'

class EquityClass:
    def __init__(self, name, csv_file = 'Ticker-SIC-Descriptions-data.csv'):
        self.name = name
        self.price_history = None
        self.sic_code = self.fetch_sic_code(csv_file)
        

    def fetch_price_history(self, start,end,tick, adjusted = 'true'):
        import requests
        
        # Define the API endpoint and your API key
        query = f"https://api.polygon.io/v2/aggs/ticker/{self.name}/range/1/{tick}/{start}/{end}?adjusted={adjusted}&sort=asc&limit=50000&apiKey=xDzNPfq0pbNuY65_75SrUqaMG1tGuguP"
        api_key = 'xDzNPfq0pbNuY65_75SrUqaMG1tGuguP'
        
        # Set up the query parameters
        params = {
            'apiKey': api_key
                }
        
        # Make the API request
        response = requests.get(query, params=params)
        
        # Check and print the response
        if response.status_code == 200:
            data = response.json()
            # print(data)
        else:
            print(f"Error: {response.status_code}")
        
        # Format the Data
        df = pd.DataFrame(data['results'])
        df['datetime'] = pd.to_datetime(df['t'], unit='ms')
        df.rename(columns = {'o':'Open','h':'High', 'l':'Low','c':'Close','datetime':'Date'}, inplace = True)
        df.set_index('Date', inplace=True)  # Use 'datetime' if the column name is 'datetime'
        df['r_close_as_frac'] =df['Close'].pct_change()
        self.price_history = df 
        
    def fetch_sic_code(self, csv_file):
        df = pd.read_csv(csv_file)
        # Assuming the CSV has columns 'equity_name' and 'sic_code'
        sic_code = df.loc[df['ticker'] == self.name, 'SIC']
        return sic_code.values[0] if not sic_code.empty else None



def compile_data(start, months, ticker,timeout=False):
    eqc = EquityClass(ticker , 'Ticker-SIC-Descriptions-data.csv')
    start = pd.to_datetime(start)
    df = pd.DataFrame()

    for i in range(0,months):    
        end = start + relativedelta(months=1, days=-1)
        end_string = end.strftime('%Y-%m-%d')
        start_string = start.strftime('%Y-%m-%d')
        print(start_string, end_string)

        eqc.fetch_price_history(start_string , end_string, tick = 'minute')
        df_new = eqc.price_history
        df = pd.concat([df, df_new])
    
        start = end + relativedelta(days=1)

        if timeout == False and i == 0:
            print('Delaying loop to remain in API constraints')
            
            
            

    return df
    
    
    

## Example usage:
# equity = EquityClass('AAPL')
# equity.fetch_price_history('2020-01-01', '2020-04-05','hour')
# print(equity.price_history)
