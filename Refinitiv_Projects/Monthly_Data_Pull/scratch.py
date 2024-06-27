import pandas as pd
import eikon as ek
from datetime import datetime, timedelta




def fetch_sector_data(api_key, rics, start_date, end_date):
    ek.set_app_key(api_key)
    fields = ['TR.ICBIndustry', 'TR.PE', 'TR.CLOSEPRICE.Date','TR.CompanyMarketCap*TR.FloatPercent/100000000' ]
    parameters = {'SDate': start_date.strftime('%Y-%m-%d'), 'EDate': end_date.strftime('%Y-%m-%d'),
                  'FRQ': 'W', 'Curn': 'USD'}
    data, err = ek.get_data(rics, fields, parameters)
    if err:
        raise Exception(f"Error fetching data: {err}")
    df = pd.DataFrame(data)
    df.rename(columns={
        'TR.COMPANYMARKETCAP*TR.FloatPercent/100000000': 'FFMC',
        'P/E (Daily Time Series Ratio)': 'P/E',
        'ICB Industry name': 'Industry'
    }, inplace=True)

    return df


def preprocess_data(df):
    # Replace empty strings with NaN
    df.replace('', pd.NA, inplace=True)
    df['Industry'].ffill(inplace=True)

    # Convert 'Date' to datetime and extract YYYY-MM-DD
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df['P/E'] = pd.to_numeric(df['P/E'], errors='coerce')
    df['FFMC'] = pd.to_numeric(df['FFMC'], errors='coerce')
    df.loc[df['P/E'].isna(), ['P/E', 'FFMC']] = 0
    # Separate the DataFrame by sector
    sectors = df['Industry'].unique()
    sector_dfs = {sector: df[df['Industry'] == sector] for sector in sectors}

    # Calculate the weighted PE for each sector
    weighted_pe_list = []
    for sector, sector_df in sector_dfs.items():
        # Group by Date and calculate the weighted PE
        weighted_pe = sector_df.groupby('Date').apply(
            lambda x: (x['P/E'] * x['FFMC']).sum() / x['FFMC'].sum()
        ).rename(sector)
        weighted_pe_list.append(weighted_pe)

    # Combine the weighted PEs into a single DataFrame
    weighted_pe_df = pd.concat(weighted_pe_list, axis=1)

    return weighted_pe_df

def get_sectors_pe(start_date, end_date):
    rics = ['2222.SE', '1120.SE', '2082.SE', '2010.SE', '1180.SE', '7010.SE', '1211.SE', '4013.SE', '1060.SE', '1150.SE',
        '1010.SE', '5110.SE', '7203.SE', '2280.SE', '2020.SE', '1080.SE', '1140.SE', '1050.SE', '8210.SE', '7202.SE',
        '7020.SE', '4250.SE', '1111.SE', '4002.SE', '4280.SE', '2050.SE', '2223.SE', '2310.SE', '8010.SE', '4263.SE',
        '2290.SE', '2382.SE', '4030.SE', '4210.SE', '4100.SE', '4164.SE', '1030.SE', '2250.SE', '2083.SE', '1020.SE',
        '4004.SE', '4190.SE', '8230.SE', '4142.SE', '4300.SE', '1212.SE', '2381.SE', '2350.SE', '4090.SE', '2380.SE',
        '4321.SE', '4200.SE', '4001.SE', '2270.SE', '4071.SE', '7030.SE', '1830.SE', '6004.SE', '4161.SE', '2330.SE',
        '4031.SE', '4015.SE', '6010.SE', '4007.SE', '4005.SE', '2060.SE', '1810.SE', '4220.SE', '4009.SE', '4020.SE',
        '4163.SE', '4003.SE', '4162.SE', '2230.SE', '4291.SE', '3020.SE', '3030.SE', '1303.SE', '2080.SE', '9526.SE',
        '2081.SE', '4260.SE', '4310.SE', '3050.SE', '1322.SE', '7200.SE', '3040.SE', '1321.SE', '4262.SE', '7204.SE',
        '2283.SE', '4322.SE', '4050.SE', '2070.SE', '3060.SE', '4150.SE', '7040.SE', '4192.SE', '1320.SE', '8030.SE',
        '1202.SE', '2190.SE', '4292.SE', '1831.SE', '3010.SE', '2300.SE', '2040.SE', '2320.SE', '3080.SE', '2281.SE',
        '4261.SE', '2170.SE', '1302.SE', '8060.SE', '3003.SE', '2120.SE', '4040.SE', '4320.SE', '8200.SE', '4290.SE',
        '4340.SE', '4323.SE', '6014.SE', '4110.SE', '6002.SE', '4012.SE', '1304.SE', '2140.SE', '8070.SE', '2200.SE',
        '1833.SE', '6001.SE', '3004.SE', '8250.SE', '3002.SE', '4081.SE', '1214.SE', '1183.SE', '4344.SE', '4347.SE',
        '4014.SE', '6070.SE', '4080.SE', '2282.SE', '2150.SE', '4008.SE', '2340.SE', '2370.SE', '4330.SE', '1182.SE',
        '8012.SE', '2030.SE', '8040.SE', '4240.SE', '2160.SE', '3091.SE', '2240.SE', '8300.SE', '8170.SE', '3090.SE',
        '3007.SE', '4180.SE', '4006.SE', '3001.SE', '4338.SE', '2100.SE', '2001.SE', '8020.SE', '1201.SE', '8120.SE',
        '4170.SE', '1301.SE', '1210.SE', '6050.SE', '3005.SE', '2090.SE', '4270.SE', '4348.SE', '8160.SE', '4011.SE',
        '1820.SE', '6090.SE', '1213.SE', '4082.SE', '7201.SE', '6040.SE', '4339.SE', '2210.SE', '4051.SE', '8270.SE',
        '6060.SE', '3008.SE', '4070.SE', '2220.SE', '4061.SE', '1832.SE', '2360.SE', '4191.SE', '2180.SE', '4345.SE',
        '8310.SE', '8150.SE', '4334.SE', '6020.SE', '6012.SE', '4140.SE', '8260.SE', '4141.SE', '4335.SE', '6013.SE',
        '2130.SE', '4130.SE', '4346.SE']
    api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
    df = fetch_sector_data(api_key, rics, start_date, end_date)
    sector_df = preprocess_data(df)
    return sector_df



if __name__ == "__main__":

    end_date = datetime.today()
    start_date = end_date - timedelta(days=5 * 365)

    print('ok')

