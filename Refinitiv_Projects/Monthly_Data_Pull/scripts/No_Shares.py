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


def pull_shares_outstanding(rics):
    start_date = '2016-12-20'
    end_date = datetime.today().strftime('%Y-%m-%d')
    fields = ['TR.SharesIssued', 'TR.SharesIssued.Date']
    parameters = {'SDate': start_date, 'EDate': end_date, 'FRQ': 'D'}

    # Fetch data
    shares_data = fetch_data(rics, fields, parameters)

    # Data cleaning and pivoting
    shares_data['Date'] = pd.to_datetime(shares_data['Date']).dt.date  # Keep only the date part
    shares_data = shares_data.groupby(['Date', 'Instrument'], as_index=False).mean()
    shares_data_pivoted = shares_data.pivot(index='Date', columns='Instrument',
                                            values='Issued')

    # Reindex to include all calendar dates
    full_date_range = pd.date_range(start=start_date, end=end_date).date

    shares_data_pivoted = shares_data_pivoted.reindex(full_date_range)
    # Convert Date index to desired format
    shares_data_pivoted.index = pd.to_datetime(shares_data_pivoted.index).strftime('%d/%m/%Y')
    shares_data_pivoted = shares_data_pivoted[rics]/1000000
    shares_data_pivoted.ffill(inplace=True)
    truncate_date = datetime(2016, 12, 31).strftime('%d/%m/%Y')
    shares_data_pivoted = shares_data_pivoted.loc[pd.to_datetime(shares_data_pivoted.index) >= pd.to_datetime(truncate_date)]

    return shares_data_pivoted


if __name__ == "__main__":
    setup_eikon_api()
    # Define RICs for shares outstanding
    rics = ['AAPL.O', 'MSFT.O']
    shares_df = pull_shares_outstanding(rics)
