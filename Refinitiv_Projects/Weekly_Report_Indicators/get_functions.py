import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from fpdf import FPDF
from datetime import datetime, timedelta
import eikon as ek
import pandas as pd
from scipy.interpolate import interp1d


maturity_dict = {
    'SAR1MZ=R': 1/12, 'SAR3MZ=R': 3/12, 'SAR6MZ=R': 6/12, 'SAR1YZ=R': 1, 'SAR2YZ=R': 2,
    'SAR5YZ=R': 5, 'SAR10YZ=R': 10, 'SAR12YZ=R': 12,
    'US1MT=RRPS': 1/12, 'US3MT=RRPS': 3/12, 'US6MT=RRPS': 6/12, 'US1YT=RRPS': 1, 'US2YT=RRPS': 2,
    'US5YT=RRPS': 5, 'US10YT=RRPS': 10, 'US20YT=RRPS': 20, 'US30YT=RRPS': 30
}

# Mapping for plot labels
maturity_labels = {
    'SAR1MZ=R': '1M', 'SAR3MZ=R': '3M', 'SAR6MZ=R': '6M', 'SAR1YZ=R': '1Y', 'SAR2YZ=R': '2Y',
    'SAR5YZ=R': '5Y', 'SAR10YZ=R': '10Y', 'SAR12YZ=R': '12Y',
    'US1MT=RRPS': '1M', 'US3MT=RRPS': '3M', 'US6MT=RRPS': '6M', 'US1YT=RRPS': '1Y', 'US2YT=RRPS': '2Y',
    'US5YT=RRPS': '5Y', 'US10YT=RRPS': '10Y', 'US20YT=RRPS': '20Y', 'US30YT=RRPS': '30Y'
}
def get_sector_data(rics, start_date, end_date):
    fields = ['TR.ICBIndustry', '(TR.CompanyMarketCap/TR.PE)*TR.FloatPercent/100',
              'TR.CLOSEPRICE.Date',
              'TR.CompanyMarketCap*TR.FloatPercent/100',
              'TR.BookValuePerShare*TR.ShrsOutCommonStock*TR.FloatPercent/100']
    parameters = {'SDate': start_date.strftime('%Y-%m-%d'), 'EDate': end_date.strftime('%Y-%m-%d'),
                  'FRQ': 'W', 'Curn': 'USD'}
    data, err = ek.get_data(rics, fields, parameters)
    if err:
        raise Exception(f"Error fetching data: {err}")
    df = pd.DataFrame(data)
    df.rename(columns={
        'TR.COMPANYMARKETCAP*TR.FloatPercent/100': 'FFMC',
        'TR.BOOKVALUEPERSHARE*TR.SHRSOUTCOMMONSTOCK*TR.FloatPercent/100': 'FFB',
        '(TR.COMPANYMARKETCAP/TR.PE)*TR.FloatPercent/100': 'FFE',
        'ICB Industry name': 'Industry'
    }, inplace=True)
    return df


def preprocess_data(df):
    # Replace empty strings with NaN
    df.replace('', pd.NA, inplace=True)
    df['Industry'].ffill(inplace=True)

    # Convert 'Date' to datetime
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['FFE'] = pd.to_numeric(df['FFE'], errors='coerce')
    df['FFMC'] = pd.to_numeric(df['FFMC'], errors='coerce')
    df['FFB'] = pd.to_numeric(df['FFB'], errors='coerce')
    # Create a complete set of unique dates
    all_dates = pd.DataFrame({'Date': df['Date'].dropna().unique()})
    # Separate the DataFrame by instrument and harmonize the dates
    instruments = df['Instrument'].unique()
    instrument_dfs = {}
    for instrument in instruments:
        instrument_df = df[df['Instrument'] == instrument]
        instrument_df = instrument_df.dropna()
        instrument_df = pd.merge(all_dates, instrument_df, on='Date', how='left')
        instrument_df.sort_values(by='Date', inplace=True)
        instrument_df['Industry'].ffill(inplace=True)
        instrument_df['Instrument'].ffill(inplace=True)
        instrument_df['FFE'].ffill(inplace=True)
        instrument_df['FFMC'].ffill(inplace=True)
        instrument_df['FFB'].ffill(inplace=True)
        instrument_dfs[instrument] = instrument_df

    # Concatenate all instrument DataFrames back into one DataFrame
    harmonized_df = pd.concat(instrument_dfs.values(), ignore_index=True)

    # Calculate the weighted PE for each industry
    industries = df['Industry'].unique()
    weighted_pe_list = []
    weighted_pb_list = []
    for industry in industries:
        industry_df = harmonized_df[harmonized_df['Industry'] == industry]
        # Group by Date and calculate the PE as the sum of FFMC divided by the sum of FFE
        pe = industry_df.groupby('Date').apply(calculate_pe).rename(industry)
        pb = industry_df.groupby('Date').apply(
            lambda x: x['FFMC'].sum() / x['FFB'].sum() if x['FFB'].sum() != 0 else float('nan')
        ).rename(industry)
        weighted_pe_list.append(pe)
        weighted_pb_list.append(pb)

    weighted_pe_df = pd.concat(weighted_pe_list, axis=1)
    weighted_pb_df = pd.concat(weighted_pb_list, axis=1)
    weighted_pe_df.index = pd.to_datetime(weighted_pe_df.index)
    weighted_pb_df.index = pd.to_datetime(weighted_pb_df.index)

    weighted_pe_df.index = weighted_pe_df.index.strftime('%Y-%m-%d')
    weighted_pb_df.index = weighted_pb_df.index.strftime('%Y-%m-%d')
    weighted_pe_df = clean_data(weighted_pe_df)
    weighted_pb_df = clean_data(weighted_pb_df)

    # Calculate cumulative sum of changes
    cumulative_sum_pe = calculate_cumulative_sum_of_changes(weighted_pe_df)
    cumulative_sum_pb = calculate_cumulative_sum_of_changes(weighted_pb_df)

    # Apply rolling window if needed
    weighted_pe_df = apply_rolling_if_needed(weighted_pe_df, cumulative_sum_pe)
    weighted_pb_df = apply_rolling_if_needed(weighted_pb_df, cumulative_sum_pb)
    return weighted_pe_df, weighted_pb_df


def apply_rolling_if_needed(df, cumulative_sum_df):
    for column in df.columns:
        final_cumulative_sum = cumulative_sum_df[column].iloc[-1]
        if final_cumulative_sum > 10:
            window_size = round(final_cumulative_sum / 10)
            df[column] = df[column].rolling(window=window_size, min_periods=1).mean()
    return df


def calculate_pe(group):
    # Calculate individual PE ratios
    pe_ratios = group['FFMC'] / group['FFE']
    # Filter out PEs that are above 100 or negative
    valid_pe_ratios = pe_ratios[(pe_ratios <= 200) & (pe_ratios >= 0)]

    # If there are no valid PEs, return NaN
    if valid_pe_ratios.empty:
        return float('nan')

    # Calculate the sum of FFMC and FFE for valid PEs
    valid_ffmc = group.loc[valid_pe_ratios.index, 'FFMC'].sum()
    valid_ffe = group.loc[valid_pe_ratios.index, 'FFE'].sum()

    # Return the average PE
    return valid_ffmc / valid_ffe if valid_ffe != 0 else float('nan')


def get_TASIsectors_pe_pb(start_date, end_date):
    sectors_pe = load_from_cache('Tsectors_pe')
    sectors_pb = load_from_cache('Tsectors_pb')
    if sectors_pe is not None and sectors_pb is not None:
        return sectors_pe, sectors_pb
    rics = [
        '1120.SE', '1180.SE', '1010.SE', '1150.SE', '1060.SE', '1050.SE', '1140.SE', '1080.SE', '8210.SE', '1111.SE',
        # Fin
        '2280.SE', '2050.SE', '4164.SE', '4001.SE', '2270.SE', '6010.SE', '4161.SE', '4163.SE', '4162.SE', '2283.SE',
        '2300.SE', '2281.SE',  # ConsStap
        '4263.SE', '4030.SE', '4100.SE', '4142.SE', '6004.SE', '4031.SE', '3030.SE', '3020.SE', '3050.SE', '2081.SE',
        '1303.SE', '3040.SE',
        '3060.SE',  # Indu
        '2010.SE', '1211.SE', '2020.SE', '2310.SE', '2290.SE', '2350.SE', '2250.SE', '2330.SE', '2060.SE', '1322.SE',
        '1321.SE',
        '1320.SE', '1202.SE', '2170.SE', '1304.SE',  # BasicM
        '6015.SE', '4210.SE', '4072.SE', '4190.SE', '4071.SE', '1830.SE', '1810.SE', '4003.SE', '4291.SE', '4262.SE',
        # ConsDis
        '2222.SE', '2223.SE', '2382.SE', '2380.SE', '2381.SE', '4200.SE', '9528.SE', '2030.SE', '9539.SE',  # Energy
        '4013.SE', '4002.SE', '9544.SE', '4004.SE', '4009.SE', '4007.SE', '4015.SE', '4005.SE', '2230.SE',
        '2070.SE', '4016.SE', '4014.SE', '9546.SE', '9530.SE',  # Health'9518.SE',
        '2082.SE', '5110.SE', '2083.SE', '2080.SE',  # Utilities
        '7010.SE', '7020.SE', '7030.SE', '7040.SE', '9542.SE', '2110.SE', '9547.SE',  # Telecom
        '7203.SE', '7202.SE', '9526.SE', '7204.SE', '7200.SE', '2370.SE', '9534.SE', '9557.SE', '7201.SE', '9550.SE',
        '9524.SE', '9538.SE', '9558.SE', '9522.SE', '9595.SE',  # Tech
        '4250.SE', '4280.SE', '4300.SE', '4090.SE', '4321.SE', '4220.SE', '4020.SE', '4310.SE', '4322.SE', '4150.SE',
        '9535.SE', '4340.SE', '4320.SE', '4342.SE', '4323.SE'  # R-E
    ]

    df = get_sector_data(rics, start_date, end_date)

    sectors_pe, sectors_pb = preprocess_data(df)
    save_to_cache(sectors_pe, 'Tsectors_pe')
    save_to_cache(sectors_pb, 'Tsectors_pb')
    return sectors_pe, sectors_pb


def get_SPsectors_pe_pb(start_date, end_date):
    sectors_pe = load_from_cache('Ssectors_pe')
    sectors_pb = load_from_cache('Ssectors_pb')
    if sectors_pe is not None and sectors_pb is not None:
        return sectors_pe, sectors_pb
    rics = [
        '1120.SE', '1180.SE', 'KFH.KW', 'FAB.AD', 'QNBK.QA', 'ENBD.DU', 'NBKK.KW', '1060.SE', '1150.SE', '1010.SE',
        # Fin
        '2280.SE', '2050.SE', '4164.SE', '2270.SE', '4001.SE', '6010.SE', 'GHITHA.AD', '4161.SE', '4163.SE', 'CSR.CS',
        # ConsStap
        'QHOLDING.AD', 'ADNOCLS.AD', 'ADPORTS.AD', 'SALIK.DU', 'QGTS.QA', '4263.SE', 'NMDC.AD', '4030.SE', 'LHM.CS',
        '4142.SE',  # Indu
        '2010.SE', '1211.SE', '2020.SE', '2310.SE', '2290.SE', '2350.SE', '2250.SE', '2330.SE', '2060.SE', '1322.SE',
        '1321.SE',
        '2010.SE', '1211.SE', 'BOROUGE.AD', 'IQCD.QA', '2020.SE', '2310.SE', 'MPHC.QA', 'FERTIGLB.AD', '2290.SE',
        '2250.SE',  # BasicM
        'AMR.AD', '4210.SE', '4190.SE', 'AIRA.DU', '4071.SE', '1830.SE', '1810.SE', '4003.SE', 'ADAVIATION.AD',
        '4291.SE',  # ConsDis
        '2222.SE', 'ADNOCGAS.AD', 'ADNOCDRILL.AD', 'ADNOCDIST.AD', '2223.SE', '2382.SE', 'QFLS.QA', '2381.SE',
        '2380.SE', '4200.SE',  # Energy
        '4013.SE', '4002.SE', '9544.SE', '4004.SE', '4009.SE', '4007.SE', '4015.SE', '4005.SE', '2230.SE',
        '4013.SE', '4002.SE', '4004.SE', 'BURJEEL.AD', '4015.SE', '4007.SE', '2230.SE', '4005.SE', '4009.SE', '2070.SE',
        # Health
        '2082.SE', 'TAQA.AD', 'DEWAA.DU', '5110.SE', 'QEWC.QA', '2083.SE', 'TQM.CS', '2080.SE', 'OQGN.OM', 'AZNOULA.KW',
        # Utilities
        '7010.SE', 'EAND.AD', '7020.SE', 'ORDS.QA', 'IAM.CS', 'DU.DU', 'ZAIN.KW', '7030.SE', 'BEYON.BH', 'OTEL.OM',
        # Telecom
        '7203.SE', '7202.SE', '9526.SE', '7204.SE', '7200.SE', '2370.SE', '9534.SE', '9557.SE', '7201.SE', '9550.SE',
        '7203.SE', '7202.SE', 'PRESIGHT.AD', '9526.SE', '7204.SE', '7200.SE', 'MEZA.QA', '2370.SE', 'OTH.TN', '7201.SE',
        # Tech
        '4250.SE', '4280.SE', '4300.SE', '4090.SE', '4321.SE', '4220.SE', '4020.SE', '4310.SE', '4322.SE', '4150.SE',
        'EMAR.DU', 'ALDAR.AD', 'EMAARDEV.DU', '4250.SE', '4280.SE', 'ERES.QA', 'TECOM.DU', 'MABK.KW', '4300.SE',
        'BRES.QA'  # R-E
    ]

    df = get_sector_data(rics, start_date, end_date)

    sectors_pe, sectors_pb = preprocess_data(df)

    sectors_pe.dropna(inplace=True)
    save_to_cache(sectors_pe, 'Ssectors_pe')
    save_to_cache(sectors_pb, 'Ssectors_pb')
    return sectors_pe, sectors_pb


def get_index_data(start_date, end_date):
    pe_df = load_from_cache('pe_df')
    pb_df = load_from_cache('pb_df')
    if pe_df is not None and pb_df is not None:
        return pe_df, pb_df

    rics = ['.SPX', '.TASI', '.QSI', '.DFMGI', '.FTFADGI', '.EGX30', '.MSX30', '.TRXFLDMEPU']
    df, err = ek.get_data(rics, ['TR.Index_PE_RTRS', 'TR.Index_PRICE_TO_BOOK_RTRS', 'TR.Index_PE_RTRS.Date'],
                          parameters={'SDate': start_date.strftime('%Y-%m-%d'), 'EDate': end_date.strftime('%Y-%m-%d'),
                                      'FRQ': 'W', 'Curn': 'USD'})

    if err:
        raise Exception(f"Error fetching data: {err}")

    df['Instrument'] = df['Instrument'].replace({
        '.TRXFLDMEPU': 'FR MENA'
    })
    # Rename columns in df1 to match df

    # Separate the combined data into P/E and P/B DataFrames
    pe_df = df[['Date', 'Instrument', 'Calculated PE Ratio']].copy()

    pe_df['Date'] = pd.to_datetime(pe_df['Date'])
    pe_df['Calculated PE Ratio'] = pd.to_numeric(pe_df['Calculated PE Ratio'], errors='coerce')
    pe_df = pe_df.groupby(['Date', 'Instrument'])['Calculated PE Ratio'].mean().reset_index()
    pivot_pe_df = pe_df.pivot(index='Date', columns='Instrument', values='Calculated PE Ratio')
    pivot_pe_df = clean_data(pivot_pe_df)

    pb_df = df[['Date', 'Instrument', 'Calculated Price to Book']].copy()

    pb_df['Date'] = pd.to_datetime(pb_df['Date'])
    pb_df['Calculated Price to Book'] = pd.to_numeric(pb_df['Calculated Price to Book'],
                                                      errors='coerce')
    pb_df = pb_df.groupby(['Date', 'Instrument'])['Calculated Price to Book'].mean().reset_index()
    pivot_pb_df = pb_df.pivot(index='Date', columns='Instrument', values='Calculated Price to Book')
    pivot_pb_df = clean_data(pivot_pb_df)

    save_to_cache(pivot_pe_df, 'pe_df')
    save_to_cache(pivot_pb_df, 'pb_df')

    return pivot_pe_df, pivot_pb_df


def get_rf_data(start_date, end_date):
    rf_df = load_from_cache('rf_df')
    if rf_df is not None:
        return rf_df

    print("Fetching risk-free rate data from Eikon")
    data, err = ek.get_data('US10YT=RR', ['TR.BIDYIELD.date', 'TR.BIDYIELD'],
                            parameters={'SDate': start_date.strftime('%Y-%m-%d'),
                                        'EDate': end_date.strftime('%Y-%m-%d'),
                                        'FRQ': 'W', 'Curn': 'USD'})
    if err:
        raise Exception(f"Error fetching data: {err}")
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Bid Yield'] = pd.to_numeric(df['Bid Yield'], errors='coerce')
    df.set_index('Date', inplace=True)

    save_to_cache(df, 'rf_df')
    return df


def clean_data(df):
    df.index = pd.to_datetime(df.index)
    df = df.ffill().bfill()
    pct_change = df.pct_change(fill_method=None)
    spikes = (pct_change > 0.2) | (pct_change < -0.2)

    for col in df.columns:
        spike_dates = spikes.index[spikes[col]].tolist()
        spike_dates = [pd.to_datetime(date) for date in spike_dates]
        for i in range(len(spike_dates) - 1):
            if (spike_dates[i + 1] - spike_dates[i]).days <= 100:
                start_date = spike_dates[i]
                end_date = spike_dates[i + 1]
                df.loc[start_date:end_date, col] = np.nan

    df.ffill(inplace=True)
    return df


def calculate_expected_returns(pe_ratios):
    return 1 / pe_ratios * 100


def get_rp_data(pe_df, risk_free_data):
    rp_df = load_from_cache('rp_df')
    if rp_df is not None:
        return rp_df
    expected_returns = calculate_expected_returns(pe_df)
    # Ensure both dataframes have Date as the index in the correct format
    expected_returns.index = pd.to_datetime(expected_returns.index).strftime('%Y-%m-%d')
    risk_free_data.index = pd.to_datetime(risk_free_data.index).strftime('%Y-%m-%d')

    # Resample the expected returns to weekly frequency using the same dates as risk_free_data
    expected_returns_weekly = expected_returns.reindex(risk_free_data.index)

    # Merge the resampled expected returns and risk-free rate data on the index (Date)
    combined_df = pd.merge(expected_returns_weekly, risk_free_data[['Bid Yield']], left_index=True, right_index=True,
                           how='inner')

    # Calculate the risk premium by subtracting the risk-free rate (Bid Yield) from the expected returns
    for col in expected_returns_weekly.columns:
        combined_df[col] = combined_df[col] - combined_df['Bid Yield']

    # Drop the 'Bid Yield' column as it's no longer needed
    combined_df.drop(columns=['Bid Yield'], inplace=True)
    save_to_cache(combined_df, 'rp_df')
    return combined_df


def generate_plot(data, title, ylabel):
    data.index = pd.to_datetime(data.index)
    data.ffill(inplace=True)

    columns_to_plot = data.columns.tolist()

    num_plots = len(columns_to_plot)
    num_cols = 3
    num_rows = (num_plots + num_cols - 1) // num_cols

    fig, axes = plt.subplots(num_rows, num_cols, figsize=(18, 6 * num_rows))
    axes = axes.flatten()

    for ax, col in zip(axes, columns_to_plot):
        ax.plot(data.index, data[col], color='darkgreen', label=col.replace('_', ' '))

        ax.set_title(f'{col.replace("_Risk_Premium", "").replace("_", " ")}')
        ax.set_xlabel('Date')
        ax.set_ylabel(ylabel)
        ax.legend()
        ax.grid(True, which='major', axis='y')

        ax.xaxis.set_major_locator(mdates.YearLocator(base=1))
        ax.tick_params(axis='x', labelsize=10)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(2)

    for ax in axes[num_plots:]:
        ax.set_visible(False)

    fig.suptitle(title)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.subplots_adjust(hspace=0.4, wspace=0.4)
    plt.show()


def save_to_cache(df, name):
    today = datetime.today().strftime('%Y-%m-%d')
    filename = f"data/{name}_{today}.csv"
    df = df.reset_index()  # Convert index to column
    df.to_csv(filename, index=False)


def load_from_cache(name):
    today = datetime.today().strftime('%Y-%m-%d')
    filename = f"data/{name}_{today}.csv"
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        df = df.set_index('Date')  # Convert column back to index
        df.index = pd.to_datetime(df.index)  # Ensure index is datetime
        return df
    return None


# Ensure output directories exist


os.makedirs(os.path.join(os.path.abspath(os.path.dirname(__file__)), "output", "png"), exist_ok=True)
os.makedirs(os.path.join(os.path.abspath(os.path.dirname(__file__)), "output", "pdf"), exist_ok=True)


def sector_plots(data, title, ylabel, filename, last):
    columns_to_plot = data.columns.tolist()
    num_plots = len(columns_to_plot)
    num_cols = 4
    num_rows = (num_plots + num_cols - 1) // num_cols  # Calculate the number of rows needed

    # Adjust figure size to ensure plots fit well in a landscape PDF page
    figure_width = 28
    figure_height = 6 * num_rows

    fig, axes = plt.subplots(num_rows, num_cols, figsize=(figure_width, figure_height))
    axes = axes.flatten()
    fig.suptitle(title, fontsize=50, color='darkgreen', ha='left', x=0.01)

    for ax, col in zip(axes, columns_to_plot):
        ax.plot(data.index, data[col], color='darkgreen', label=col.replace('_', ' '))

        last_rel_value = data[col].iloc[-1]
        last_true_value = data[col].iloc[-1] * last
        ax.axhline(y=last_rel_value, color='red', linestyle='--', linewidth=1)

        ax.set_title(f'{col.replace(".", "")}, {ylabel.split()[-1]}: {last_true_value:.2f}x', fontsize=15)
        ax.set_xlabel('Date')
        ax.set_ylabel(f'{ylabel} (x)')
        ax.grid(True, which='major', axis='y')

        ax.xaxis.set_major_locator(mdates.YearLocator(base=1))
        ax.tick_params(axis='x', labelsize=10)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

        # Add 'x' next to y-axis tick values
        y_labels = [f'{label:.2f}x' for label in ax.get_yticks()]
        ax.set_yticklabels(y_labels)

        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(2)

    for ax in axes[num_plots:]:
        ax.set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.subplots_adjust(hspace=0.2, wspace=0.2)

    png_filename = os.path.join(os.path.join(os.path.abspath(os.path.dirname(__file__)), "output", "png"),
                                f'{filename}.png')
    fig.savefig(png_filename, bbox_inches='tight')
    plt.close(fig)

    return png_filename


def sector_pngs(Tsectors_rel_pe, Tsectors_rel_pb, Ssectors_rel_pe, Ssectors_rel_pb, T_lastvalues, S_lastvalues):
    Tpe_png = sector_plots(Tsectors_rel_pe, 'TASI Sectors Relative P/E Ratios', 'Relative P/E Ratio', 'Tsectors_rel_pe',
                           T_lastvalues[0])
    Tpb_png = sector_plots(Tsectors_rel_pb, 'TASI Sectors Relative P/B Ratios', 'Relative P/B Ratio', 'Tsectors_rel_pb',
                           T_lastvalues[1])
    Spe_png = sector_plots(Ssectors_rel_pe, 'S&P Pan Arab Sectors Relative P/E Ratios', 'Relative P/E Ratio',
                           'Ssectors_rel_pe', S_lastvalues[0])
    Spb_png = sector_plots(Ssectors_rel_pb, 'S&P Pan Arab Sectors Relative P/B Ratios', 'Relative P/B Ratio',
                           'Ssectors_rel_pb', S_lastvalues[1])
    return Tpe_png, Tpb_png, Spe_png, Spb_png


def index_plots(data, title, ylabel, filename):
    columns_to_plot = data.columns.tolist()
    num_plots = len(columns_to_plot)
    num_cols = 3
    num_rows = (num_plots + num_cols - 1) // num_cols  # Calculate the number of rows needed

    # Adjust figure size to ensure plots fit well in a landscape PDF page
    figure_width = 21
    figure_height = 6 * num_rows

    fig, axes = plt.subplots(num_rows, num_cols, figsize=(figure_width, figure_height))
    axes = axes.flatten()
    fig.suptitle(title, fontsize=50, color='darkgreen', ha='left', x=0.01)

    for ax, col in zip(axes, columns_to_plot):
        ax.plot(data.index, data[col], color='darkgreen', label=col.replace('_', ' '))

        last_value = data[col].iloc[-1]
        ax.axhline(y=last_value, color='red', linestyle='--', linewidth=1)
        unit = 'x' if ylabel.split()[-1] == 'Ratio' else '%'
        ax.set_title(f'{col.replace(".", "")}: {last_value:.2f}{unit}', fontsize=15)
        ax.set_xlabel('Date')
        ax.set_ylabel(f'{ylabel}')
        ax.grid(True, which='major', axis='y')

        ax.xaxis.set_major_locator(mdates.YearLocator(base=1))
        ax.tick_params(axis='x', labelsize=10)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

        # Add 'x' next to y-axis tick values
        y_labels = [f'{label:.2f}{unit}' for label in ax.get_yticks()]
        ax.set_yticklabels(y_labels)

        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(2)

    for ax in axes[num_plots:]:
        ax.set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.subplots_adjust(hspace=0.2, wspace=0.2)

    png_filename = os.path.join(os.path.join(os.path.abspath(os.path.dirname(__file__)), "output", "png"),
                                f'{filename}.png')
    fig.savefig(png_filename, bbox_inches='tight')
    plt.close(fig)

    return png_filename


def index_pngs(index_rp, index_pe, index_pb):
    riskpremium_png = index_plots(index_rp, 'Market Risk Premiums', 'Risk Premium', 'index_rp')
    pe_png = index_plots(index_pe, 'Market P/E Ratios', 'P/E Ratio', 'index_pe')
    pb_png = index_plots(index_pb, 'Market P/B Ratios', 'P/B Ratio', 'index_pb')

    return riskpremium_png, pe_png, pb_png


def create_pdf_from_png(png_files, pdf_file):
    pdf = FPDF(orientation='L', unit='in', format='A4')
    for png_file in png_files:
        pdf.add_page()
        pdf.image(png_file, x=0.5, y=0.5, w=10, h=7)
    pdf.output(pdf_file)


def calculate_cumulative_sum_of_changes(df):
    mean_values = df.mean()
    normalized_changes = df.diff().abs().div(mean_values)
    cumulative_sum_of_normalized_changes = normalized_changes.cumsum()
    return cumulative_sum_of_normalized_changes


def calculate_relative_pe_pb(Tsectors_pe, Tsectors_pb, index_pe, index_pb, name):
    # Ensure the dates are in the same format

    Tsectors_pe.index = pd.to_datetime(Tsectors_pe.index)
    Tsectors_pb.index = pd.to_datetime(Tsectors_pb.index)

    # Localize timezone-naive timestamps to UTC, then convert to timezone-naive
    if index_pe.index.tz is None:
        index_pe.index = pd.to_datetime(index_pe.index).tz_localize('UTC').tz_convert(None)
    else:
        index_pe.index = pd.to_datetime(index_pe.index).tz_convert(None)

    if index_pb.index.tz is None:
        index_pb.index = pd.to_datetime(index_pb.index).tz_localize('UTC').tz_convert(None)
    else:
        index_pb.index = pd.to_datetime(index_pb.index).tz_convert(None)
    # Align the DataFrames by dates
    index_pe = index_pe.reindex(Tsectors_pe.index)
    index_pb = index_pb.reindex(Tsectors_pb.index)

    # Perform the division
    Tsectors_rel_pe = Tsectors_pe.div(index_pe[name], axis=0)
    Tsectors_rel_pb = Tsectors_pb.div(index_pb[name], axis=0)

    return Tsectors_rel_pe, Tsectors_rel_pb


def generate_combined_pdf(api_key, output_pdf_file):
    ek.set_app_key(api_key)
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5 * 365)
    # Fetch data
    index_pe, index_pb = get_index_data(start_date, end_date)
    rf_df = get_rf_data(start_date, end_date)
    index_rp = get_rp_data(index_pe, rf_df)
    Tsectors_pe, Tsectors_pb = get_TASIsectors_pe_pb(start_date, end_date)
    Ssectors_pe, Ssectors_pb = get_SPsectors_pe_pb(start_date, end_date)
    # Generate PNG file
    Tsectors_rel_pe, Tsectors_rel_pb = calculate_relative_pe_pb(Tsectors_pe, Tsectors_pb, index_pe, index_pb, '.TASI')
    Ssectors_rel_pe, Ssectors_rel_pb = calculate_relative_pe_pb(Ssectors_pe, Ssectors_pb, index_pe, index_pb, 'FR MENA')
    T_lastvalues = (index_pe['.TASI'].iloc[-1], index_pb['.TASI'].iloc[-1])
    S_lastvalues = (index_pe['FR MENA'].iloc[-1], index_pb['FR MENA'].iloc[-1])
    ind_rp_png, ind_pe_png, ind_pb_png = index_pngs(index_rp, index_pe, index_pb)
    Tsec_pe_png, Tsec_pb_png, Ssec_pe_png, Ssec_pb_png = sector_pngs(Tsectors_rel_pe, Tsectors_rel_pb, Ssectors_rel_pe,
                                                                     Ssectors_rel_pb, T_lastvalues, S_lastvalues)
    png_files = [ind_rp_png, ind_pe_png, ind_pb_png, Tsec_pe_png, Tsec_pb_png, Ssec_pe_png, Ssec_pb_png]
    create_pdf_from_png(png_files, output_pdf_file)


def get_fx(start_date, end_date):
    fx_data, err = ek.get_data(['EUR=', 'EGP=', 'KWD=', 'TRY=', 'GBP=', 'XAU='],
                               ['TR.MIDPRICE', 'TR.MIDPRICE.Date'],
                               parameters={'SDate': start_date.strftime('%Y-%m-%d'),
                                           'EDate': end_date.strftime('%Y-%m-%d'),
                                           'FRQ': 'W'})
    # Example implementation for fetching FX data
    fx_data['Date'] = pd.to_datetime(fx_data['Date']).dt.date
    fx_data['Instrument'] = fx_data['Instrument'].str.replace('=', '')
    fx_data = fx_data.groupby(['Date', 'Instrument'], as_index=False).mean()
    fx_data_pivoted = fx_data.pivot(index='Date', columns='Instrument', values='Mid Price')

    fx_data_pivoted['GBP'] = 1 / fx_data_pivoted['GBP']

    fx_data_pivoted['EUR'] = 1 / fx_data_pivoted['EUR']
    # Forward fill missing values
    fx_data_pivoted.ffill(inplace=True)

    return fx_data_pivoted  # Placeholder for actual FX data


def fx_png(fx_df):
    columns_to_plot = fx_df.columns.tolist()
    num_plots = len(columns_to_plot)
    num_cols = 4
    num_rows = 2  # Calculate the number of rows needed

    # Adjust figure size to ensure plots fit well in a landscape PDF page
    figure_width = 28
    figure_height = 6 * num_rows

    fig, axes = plt.subplots(num_rows, num_cols, figsize=(figure_width, figure_height))
    axes = axes.flatten()
    fig.suptitle('FX Data', fontsize=50, color='darkgreen', ha='left', x=0.01)

    for ax, col in zip(axes, columns_to_plot):
        ax.plot(fx_df.index, fx_df[col], color='darkgreen', label=col.replace('_', ' '))

        last_value = fx_df[col].iloc[-1]
        ax.axhline(y=last_value, color='red', linestyle='--', linewidth=1)

        if col == 'XAU':
            ax.set_title(f'1 kg Gold = {last_value:.2f} USD', fontsize=15)
            ax.set_ylabel('Price per kg')
        else:
            ax.set_title(f'1 USD = {last_value:.2f} {col}', fontsize=15)
            ax.set_ylabel('1 USD =')

        ax.set_xlabel('Date')
        ax.grid(True, which='major', axis='y')

        ax.xaxis.set_major_locator(mdates.YearLocator(base=1))
        ax.tick_params(axis='x', labelsize=10)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

        # Remove 'x' next to y-axis tick values
        y_labels = [f'{label:.2f}' for label in ax.get_yticks()]
        ax.set_yticklabels(y_labels)

        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(2)

    for ax in axes[num_plots:]:
        ax.set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.subplots_adjust(hspace=0.2, wspace=0.2)

    output_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "output", "png")
    os.makedirs(output_dir, exist_ok=True)
    png_filename = os.path.join(output_dir, 'fx.png')
    fig.savefig(png_filename, bbox_inches='tight')
    plt.close(fig)

    return png_filename


def get_rates(start_date, end_date):
    # Example implementation for fetching rates data
    yield_SAR, err = ek.get_data([ 'SAR1MZ=R', 'SAR3MZ=R', 'SAR6MZ=R', 'SAR1YZ=R', 'SAR2YZ=R','SAR5YZ=R','SAR10YZ=R','SAR12YZ=R'],
                               ['CF_CLOSE'],
                               parameters={'SDate': 0})
    yield_USD, err = ek.get_data(['US1MT=RRPS', 'US3MT=RRPS', 'US6MT=RRPS', 'US1YT=RRPS', 'US2YT=RRPS', 'US5YT=RRPS','US10YT=RRPS','US20YT=RRPS','US30YT=RRPS'],
                               ['CF_YIELD'],
                               parameters={'SDate': 0})

    spread_df, err  = ek.get_data(['SASAR3MD=','USD3MFSR=X'],
                ['TR.FIXINGVALUE', 'TR.FIXINGVALUE.Date'],
                parameters={'SDate': start_date.strftime('%Y-%m-%d'),
                            'EDate': end_date.strftime('%Y-%m-%d'),
                            'FRQ': 'W'})
    spread_df['Date'] = pd.to_datetime(spread_df['Date'])

    # Pivot the DataFrame
    spread_df_pivoted = spread_df.pivot(index='Date', columns='Instrument', values='Fixing Value')
    spread_df_pivoted.ffill(inplace=True)
    # Add your logic to fetch rates data here
    rates_data = "Sample Rates Data"
    #plot_yield_curve(yield_USD,'US Treasury Yield Curve')
    #plot_yield_curve(yield_SAR,'SAR Yield Curve')# Placeholder for actual rates data
    plot_spreads(spread_df_pivoted)
    return rates_data

def plot_spreads(spread_df_pivoted):
    # Extract SAIBOR 3M and LIBOR 3M data
    saibor_3m = spread_df_pivoted['SASAR3MD=']
    libor_3m = spread_df_pivoted['USD3MFSR=X']

    # Calculate the spread
    spread = saibor_3m - libor_3m

    # Plot SAIBOR 3M and LIBOR 3M
    plt.figure(figsize=(14, 7))
    plt.plot(spread_df_pivoted.index, saibor_3m, label='SAIBOR 3M', color='blue')
    plt.plot(spread_df_pivoted.index, libor_3m, label='LIBOR 3M', color='red')
    plt.xlabel('Date')
    plt.ylabel('Rate')
    plt.title('SAIBOR 3M vs LIBOR 3M')
    plt.legend()
    plt.grid(True)
    plt.show()

    # Plot the spread
    plt.figure(figsize=(14, 7))
    plt.plot(spread_df_pivoted.index, spread, label='Spread (SAIBOR 3M - LIBOR 3M)', color='green')
    plt.xlabel('Date')
    plt.ylabel('Spread')
    plt.title('Spread between SAIBOR 3M and LIBOR 3M')
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_yield_curve(yield_data, title):
    # Extract maturities using the hardcoded dictionary
    yield_data['Maturity'] = yield_data['Instrument'].map(maturity_dict)
    maturities = yield_data['Maturity']
    yields = yield_data['CF_CLOSE'] if 'CF_CLOSE' in yield_data.columns else yield_data['CF_YIELD']

    # Sort by maturity
    sorted_indices = np.argsort(maturities)
    maturities = maturities.iloc[sorted_indices]
    yields = yields.iloc[sorted_indices]

    # Interpolate for a smooth curve
    interpolation = interp1d(maturities, yields, kind='cubic')
    smooth_maturities = np.linspace(maturities.min(), maturities.max(), 500)
    smooth_yields = interpolation(smooth_maturities)

    # Apply power transformation
    power = 0.5  # Adjust this value to control the transformation strength
    transformed_maturities = smooth_maturities ** power
    original_maturities_transformed = maturities ** power

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(transformed_maturities, smooth_yields, label='Interpolated Yield Curve', color='darkgreen')
    plt.scatter(original_maturities_transformed, yields, color='red', zorder=5)

    # Annotate maturities on the x-axis
    plt.xticks(original_maturities_transformed,
               [maturity_labels[inst] for inst in yield_data['Instrument'].iloc[sorted_indices]], rotation=45)
    plt.xlabel('Maturity')
    plt.ylabel('Yield (%)')
    plt.title(title)
    plt.grid(True, which='major', axis='x', linestyle='--', linewidth=0.7)
    plt.show()

# Call the main function
if __name__ == "__main__":
    ek.set_app_key('d1591ca6f45645a7bfc517785524647492c13cbc')
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5 * 365)
    #fx_df = get_fx(start_date, end_date)
    #fx_png(fx_df)

    rates_data = get_rates(start_date, end_date)
    print(f"Rates Data: {rates_data}")
