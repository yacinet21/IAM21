import eikon as ek
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import os
import base64
from io import BytesIO
import math
import matplotlib.dates as mdates
import re
# Set up Eikon API key
api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
ek.set_app_key(api_key)


def setup_eikon_api(api_key):
    ek.set_app_key(api_key)


def fetch_sp_panarab_timeseries(start_date):
    today = datetime.today().strftime('%Y-%m-%d')
    file_name = f'raw_data/SPPAUT_returns_{today}.csv'

    if os.path.exists(file_name):
        df = pd.read_csv(file_name, index_col=0, parse_dates=True)
        return df

    df = ek.get_timeseries('.SPPAUT', ['CLOSE'], interval='weekly', start_date=start_date.strftime('%Y-%m-%d'))
    df.index = pd.to_datetime(df.index).normalize()  # Ensure index is timezone-naive
    df['Return'] = df['CLOSE'].pct_change().dropna()
    df.to_csv(file_name)

    return df


def fetch_ric_data(rics, start_date, end_date):
    df, err = ek.get_data(rics, ['CF_NAME', 'TR.TotalReturn1Wk', 'TR.TotalReturn1Wk.Date', 'TR.CompanyMarketCap'],
                          parameters={'SDate': start_date.strftime('%Y-%m-%d'), 'EDate': end_date.strftime('%Y-%m-%d'),
                                      'FRQ': 'W', 'Curn': 'USD'})
    if err:
        raise Exception(f"Error fetching data: {err}")
    df['CF_NAME'] = df['CF_NAME'].apply(
        lambda x: re.sub(r'/d', '', x) if isinstance(x, str) else x)
    df.replace('NaN', np.nan, inplace=True)
    df['Date'] = df['Date'].apply(lambda x: x.split('T')[0])
    df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
    df.set_index('Date', inplace=True)
    df['1 Week Total Return'] = pd.to_numeric(df['1 Week Total Return'], errors='coerce')
    df['Company Market Cap'] = pd.to_numeric(df['Company Market Cap'], errors='coerce')
    df.fillna(0, inplace=True)
    df['Return'] = df['1 Week Total Return'] / 100
    return df


def calculate_relative_returns(sp_panarab_df, ric_df):
    # Ensure the dates in sp_panarab_df are unique and sorted
    sp_dates = sp_panarab_df.index.unique().sort_values()
    ric_df.replace('', np.nan, inplace=True)
    ric_df['CF_NAME'] = ric_df['CF_NAME'].ffill()
    ric_df['CF_NAME'] = ric_df['CF_NAME'].apply(
        lambda x: ' '.join(x.split()[:2]) if isinstance(x, str) else x)

    # Pivot the ric_df
    pivoted_ric_df = ric_df.pivot_table(index=ric_df.index, columns='CF_NAME', values='Return')

    # Function to find the closest date
    def find_closest_date(date, reference_dates):
        idx = reference_dates.get_indexer([date], method='nearest')
        return reference_dates[idx][0]

    # Create a mapping of pivoted_ric_df dates to the closest sp_panarab_df dates
    closest_dates = pivoted_ric_df.index.to_series().apply(lambda x: find_closest_date(x, sp_dates))

    # Update the index of pivoted_ric_df to closest dates
    pivoted_ric_df.index = closest_dates

    # Aggregate by keeping the first non-NaN value for each ticker and date
    def first_non_nan(series):
        return series.dropna().iloc[0] if not series.dropna().empty else np.nan

    aggregated_ric_df = pivoted_ric_df.groupby(pivoted_ric_df.index).apply(lambda x: x.apply(first_non_nan))

    # Align the pivoted_ric_df with sp_panarab_df
    sp_returns = sp_panarab_df[['Return']]
    relative_returns = aggregated_ric_df.subtract(sp_returns['Return'], axis=0)
    relative_returns.fillna(0, inplace=True)
    # Drop rows with NaN values

    return relative_returns


def initialize_html_content():
    return '<html><head><title>Price Paths</title></head><body>'


def finalize_html_content(html_content):
    html_content += '</body></html>'
    return html_content


def generate_plot(price_path, instrument, ax):
    ax.plot(price_path.index, price_path, label=instrument, color='darkgreen', linewidth = 0.75)
    last_value = price_path.iloc[-1]
    ax.axhline(y=last_value, color='red', linestyle='--', linewidth=1)
    ax.set_title(f'{instrument[:21]} / S&P Pan Arab', fontsize=6)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%y-%m'))
    ax.yaxis.set_major_locator(plt.MaxNLocator(10))  # Major ticks
    ax.grid(True, which='major', axis='y')
    ax.tick_params(axis='x', labelsize=5)  # Smaller x-tick label font size
    ax.tick_params(axis='y', labelsize=5)  # Smaller y-tick label font size

    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', fontsize=5)  # Rotate dates diagonally


def save_plot_as_base64(fig, dpi=300):
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=dpi)
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    buffer.close()
    plt.close(fig)
    return image_base64


def generate_page(relative_returns, company_names, start_idx, end_idx):
    fig, axs = plt.subplots(6, 4, figsize=(8.27, 11.69))  # A4 size in inches (landscape orientation)
    axs = axs.flatten()

    # Determine the number of instruments to plot
    num_instruments = min(len(company_names[start_idx:end_idx]), len(company_names))
    for i in range(num_instruments):
        ax = axs[i]

        instrument = relative_returns.columns[start_idx + i]
        price_path = (relative_returns[instrument] + 1).cumprod() * 100
        # Ensure price_path is not empty and prepend the value 100
        if not price_path.empty:
            first_date = price_path.index[0]
            new_date = first_date - pd.Timedelta(days=7)
            price_path = pd.concat([pd.Series([100], index=[new_date]), price_path])

        generate_plot(price_path, instrument, ax)

    # Hide unused subplots
    for j in range(num_instruments, len(axs)):
        fig.delaxes(axs[j])

    plt.tight_layout()
    return save_plot_as_base64(fig)


def generate_graphs_html(api_key, rics, sector):
    # Set up dates
    end_date = datetime.today()
    start_date = end_date - timedelta(days=2 * 365)  # Two years ago

    # Define file path for cached RIC data
    ric_csv = f'raw_data/graph_{sector}.csv'

    # Setup Eikon API
    setup_eikon_api(api_key)

    # Fetch or load RIC data
    if os.path.exists(ric_csv):
        print(f"Loading RIC data for sector {sector} from cache.")
        ric_df = pd.read_csv(ric_csv, index_col='Date', parse_dates=True)
    else:
        print(f"Fetching RIC data for sector {sector}.")
        ric_df = fetch_ric_data(rics, start_date, end_date)
        ric_df.to_csv(ric_csv)

    # Fetch S&P Pan Arab data
    ric_df['CF_NAME'] = ric_df['CF_NAME'].ffill()
    latest_market_caps = ric_df.groupby('CF_NAME')['Company Market Cap'].last()
    sorted_rics = latest_market_caps.sort_values(ascending=False).index.tolist()

    # Ensure the data is ordered by market cap and then by date
    ric_df['CF_NAME'] = pd.Categorical(
        ric_df['CF_NAME'],
        categories=sorted_rics,
        ordered=True
    )
    ric_df.sort_values(['CF_NAME', 'Date'], inplace=True)

    # Fetch S&P Pan Arab data
    sp_panarab_df = fetch_sp_panarab_timeseries(start_date)

    # Calculate relative returns
    relative_returns = calculate_relative_returns(sp_panarab_df, ric_df)

    # Determine the number of pages needed
    plots_per_page = 24
    num_pages = math.ceil(len(rics) / plots_per_page)

    # Initialize the HTML content
    html_content = initialize_html_content()

    for page in range(num_pages):
        start_idx = page * plots_per_page
        end_idx = min(start_idx + plots_per_page, len(relative_returns.columns))
        image_base64 = generate_page(relative_returns, relative_returns.columns, start_idx, end_idx)
        html_content += f'<div style="page-break-after: always;"><img src="data:image/png;base64,{image_base64}" alt="Page {page + 1}"></div>'

    html_content = finalize_html_content(html_content)

    html_file = 'htmls/price_paths.html'
    with open(html_file, 'w') as f:
        f.write(html_content)

    return html_file


if __name__ == "__main__":
    api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
    rics = ['GOOG.O', 'AAPL.O', 'MSFT.O', 'TSLA.O', 'IBM.N', 'NFLX.O', 'AMZN.O', 'META.O', 'NVDA.O', 'INTC.O', 'ORCL.O',
            'CSCO.O']
    html_file = generate_graphs_html(api_key, rics)
    print(f"Generated HTML file: {html_file}")
