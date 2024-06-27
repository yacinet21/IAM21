import pandas as pd
from scripts.fx_rates import pull_fx_rates
from scripts.No_Shares import pull_shares_outstanding
from scripts.Volumes import pull_volumes
from scripts.prices import pull_prices
import eikon as ek
import json


def main():
    # Define RICs and currencies
    with open('../companies.json', 'r') as f:
        loaded_companies = json.load(f)
    rics = [info["RIC Ticker"] for info in loaded_companies.values()]
    currencies = ['AED=', 'QAR=', 'SAR=', 'KWD=', 'OMR=', 'GBP=', 'EGP=']
    # Pull data
    fx_df = pull_fx_rates(currencies)
    shares_df = pull_shares_outstanding([ric for ric in rics if ric != 'KSA'])
    shares_df['KSA'] = None  # Add an empty column for KSA shares
    volumes_df = pull_volumes(rics)
    prices_df = pull_prices(rics)

    # Helper function to add company names and BBG tickers
    def add_metadata(df, loaded_companies):
        company_names = []
        bbg_tickers = []
        for ric in df.columns:
            for company, info in loaded_companies.items():
                if info["RIC Ticker"] == ric:
                    company_names.append(company)
                    bbg_tickers.append(info["BBG Ticker"])
                    break
            else:
                company_names.append("")
                bbg_tickers.append("")
        df.columns = pd.MultiIndex.from_tuples(zip(company_names, bbg_tickers))

    # Add metadata to DataFrames
    add_metadata(shares_df, loaded_companies)
    add_metadata(volumes_df, loaded_companies)
    add_metadata(prices_df, loaded_companies)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    output_file = 'data/financial_data.xlsx'
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        fx_df.to_excel(writer, sheet_name='FX Rates')
        shares_df.to_excel(writer, sheet_name='Shares Outstanding')
        volumes_df.to_excel(writer, sheet_name='Volumes')
        prices_df.to_excel(writer, sheet_name='Prices')
    print(f'Data successfully written to {output_file}')


if __name__ == "__main__":
    api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
    ek.set_app_key(api_key)
    main()
