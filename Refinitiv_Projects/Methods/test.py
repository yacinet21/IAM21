import eikon as ek
import pandas as pd

# Set up Eikon API key
api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
ek.set_app_key(api_key)

# List of tickers

# Fetch data
data = ek.get_timeseries('TC-SHA-SSZ', ['CLOSE'],  start_date='2023-01-01')

# Check for errors

# Save data to a DataFrame
df = pd.DataFrame(data)

# Print the DataFrame
print(df)
excel_file = 'financial_data.xlsx'
df.to_excel(excel_file, index=False)
print(f"DataFrame saved to {excel_file}")