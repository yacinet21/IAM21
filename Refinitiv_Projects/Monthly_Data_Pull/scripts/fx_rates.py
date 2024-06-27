import eikon as ek
import pandas as pd
from datetime import datetime
import os

def setup_eikon_api():
    api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
    ek.set_app_key(api_key)

def fetch_data(rics, fields, parameters):
    print("Fetching data from Eikon")
    data, err = ek.get_data(rics, fields, parameters)
    if err:
        raise Exception(f"Error fetching data: {err}")
    df = pd.DataFrame(data)
    return df

def pull_fx_rates(rics):

    start_date = '2016-12-31'
    end_date = datetime.today().strftime('%Y-%m-%d')
    fields = ['TR.MIDPRICE', 'TR.MIDPRICE.Date']
    parameters = {'SDate': start_date, 'EDate': end_date, 'FRQ': 'D'}

    # Fetch data
    fx_data = fetch_data(rics, fields, parameters)

    # Data cleaning and pivoting
    fx_data['Date'] = pd.to_datetime(fx_data['Date']).dt.date
    fx_data['Instrument'] = fx_data['Instrument'].str.replace('=', '')
    fx_data = fx_data.groupby(['Date', 'Instrument'], as_index=False).mean()
    fx_data_pivoted = fx_data.pivot(index='Date', columns='Instrument', values='Mid Price')

    full_date_range = pd.date_range(start=start_date, end=end_date).date
    fx_data_pivoted = fx_data_pivoted.reindex(full_date_range)

    # Forward fill missing values
    fx_data_pivoted.ffill(inplace=True)
    fx_data_pivoted.bfill(inplace=True)

    fx_data_pivoted.index = pd.to_datetime(fx_data_pivoted.index).strftime('%d/%m/%Y')
    return fx_data_pivoted



if __name__ == "__main__":
    setup_eikon_api()
    currencies = ['AED=', 'QAR=', 'SAR=', 'KWD=', 'OMR=', 'GBP=', 'EGP=']
    fx_df= pull_fx_rates(currencies)

