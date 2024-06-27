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



def pull_volumes(rics):
    start_date = '2016-12-20'
    end_date = datetime.today().strftime('%Y-%m-%d')
    fields = ['TR.ACCUMULATEDVOLUME', 'TR.ACCUMULATEDVOLUME.Date']
    parameters = {'SDate': start_date, 'EDate': end_date, 'FRQ': 'D', 'Adjusted':'0'}

    # Fetch data
    vols_data = fetch_data(rics, fields, parameters)

    # Convert the Date column to datetime and keep only the date part
    vols_data['Date'] = pd.to_datetime(vols_data['Date']).dt.date

    # Group by Date and Instrument and calculate the mean
    vols_data = vols_data.groupby(['Date', 'Instrument'], as_index=False).mean()

    # Pivot the DataFrame
    vols_data_pivoted = vols_data.pivot(index='Date', columns='Instrument', values='Accumulated Volume')

    # Reindex to include all calendar dates
    full_date_range = pd.date_range(start=start_date, end=end_date).date
    vols_data_pivoted = vols_data_pivoted.reindex(full_date_range)

    # Forward fill missing values
    vols_data_pivoted.ffill(inplace=True)

    # Convert Date index to desired format
    vols_data_pivoted.index = pd.to_datetime(vols_data_pivoted.index).strftime('%d/%m/%Y')

    # Truncate rows at 31/12/2016 (ensure correct date format)
    truncate_date = datetime(2016, 12, 31).strftime('%d/%m/%Y')
    vols_data_pivoted = vols_data_pivoted.loc[pd.to_datetime(vols_data_pivoted.index) >= pd.to_datetime(truncate_date)]

    # Reorder the columns based on rics
    vols_data_pivoted = vols_data_pivoted[rics]

    return vols_data_pivoted


if __name__ == "__main__":
    setup_eikon_api()
    # Define RICs for volumes
    rics = ['AAPL.O', 'MSFT.O']
    volumes_df = pull_volumes(rics)
