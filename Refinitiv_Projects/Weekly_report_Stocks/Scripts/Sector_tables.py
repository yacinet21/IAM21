import eikon as ek
import pandas as pd
import numpy as np
from datetime import datetime
import pdfkit as pdf
import os
from datetime import timedelta
import re


# Function to set up Eikon API
def setup_eikon_api(api_key):
    ek.set_app_key(api_key)
    pd.set_option("display.max_columns", None)


# Function to fetch data from Refinitiv
def fetch_data(rics, fields, parameters):
    print("Fetching data from Eikon")
    data, err = ek.get_data(rics, fields, parameters)
    if err:
        raise Exception(f"Error fetching data: {err}")
    df = pd.DataFrame(data)
    return df


# Function to preprocess the DataFrame
def preprocess_dataframe(df):
    df.drop(columns=['Date', 'Instrument'], inplace=True)
    df.columns = [
        'Ticker', 'Name', 'Industry', 'Mkt Cap', 'Last Price',
        'Avg 3M Volume', '1Wk', '1Mo', '3Mo',
        'YTD', 'P/B', 'DY', 'P/E LTM', 'P/E NTM', 'P/E FY1'
    ]
    # Convert Mkt Cap and Avg 3M Volume to M and B
    df['Mkt Cap'] = pd.to_numeric(df['Mkt Cap'], errors='coerce') / 1e9
    df['Avg 3M Volume'] = pd.to_numeric(df['Avg 3M Volume'], errors='coerce') / 1e6
    df['Name'] = df['Name'].apply(
        lambda x: re.sub(r'/d', '', x) if isinstance(x, str) else x)

    return df


def highlight_top_bottom(s, min_green=False):
    # Strip any non-numeric characters and convert to numeric if possible
    s_numeric = s.replace({'%': '', 'x': ''}, regex=True).apply(pd.to_numeric, errors='coerce')
    n = len(s_numeric)
    threshold = max(int(n * 0.1), 2)  # Calculate 10% of the values, ensure at least 2 for threshold
    if n > 2:
        if min_green:
            is_max = s_numeric.nlargest(threshold).index
            is_min = s_numeric.nsmallest(threshold).index
            return [
                'background-color: lightsalmon' if i in is_max else 'background-color: palegreen' if i in is_min else ''
                for i in s.index]
        else:
            is_max = s_numeric.nlargest(threshold).index
            is_min = s_numeric.nsmallest(threshold).index
            return [
                'background-color: palegreen' if i in is_max else 'background-color: lightsalmon' if i in is_min else ''
                for i in s.index]
    else:
        return [''] * n


# Function to style the DataFrame
def style_dataframe(df):
    # Remove the index by resetting it and then dropping it
    df.reset_index(drop=True, inplace=True)
    # Define the columns for formatting
    other_columns = ['Mkt Cap', 'Last Price', 'Avg 3M Volume']
    percentage_columns = ['1Wk', '1Mo', '3Mo', 'YTD', 'DY']
    multiple_columns = ['P/B', 'P/E LTM', 'P/E NTM', 'P/E FY1']
    pb_pe_columns = multiple_columns  # These columns will have switched highlighting
    text_columns = ['Industry', 'Name', 'Ticker']  # Columns that should be left-aligned

    # Replace 'NaN' strings with actual NaN values and convert columns to numeric
    df.replace('NaN', np.nan, inplace=True)
    for col in percentage_columns + multiple_columns + other_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['P/B'] = df['P/B'].apply(lambda x: np.nan if x < 0 else x)
    df['P/E LTM'] = df['P/E LTM'].apply(lambda x: np.nan if x < 0 else x)
    df['P/E NTM'] = df['P/E NTM'].apply(lambda x: np.nan if x < 0 else x)
    df['P/E FY1'] = df['P/E FY1'].apply(lambda x: np.nan if x < 0 else x)
    # Create a format dictionary for the specific columns
    format_dict = {col: "{:.1f}%" for col in percentage_columns}
    format_dict.update({col: "{:.1f}" for col in other_columns})
    format_dict.update({col: "{:.1f}x" for col in multiple_columns})

    # Apply the formatting
    for col, fmt in format_dict.items():
        df[col] = df[col].map(lambda x: fmt.format(x) if not pd.isna(x) else '-')

    # Generate the HTML table with highlighting
    html = '<table>'
    html += '<thead><tr>'
    for col in df.columns:
        alignment = 'right' if col not in text_columns else 'left'
        html += f'<th style="text-align: {alignment};">{col}</th>'
    html += '</tr></thead>'
    html += '<tbody>'
    for idx, row in df.iterrows():
        html += '<tr>'
        for col in df.columns:
            style = ''
            if col in percentage_columns + multiple_columns:  # Apply highlighting only to percentage and multiple columns
                if col in pb_pe_columns:
                    style = highlight_top_bottom(df[col], min_green=True)[
                        idx]  # Apply the highlighting for P/B and P/E columns
                else:
                    style = highlight_top_bottom(df[col])[idx]  # Apply the highlighting to numeric columns
            alignment = 'right' if col not in text_columns else 'left'
            html += f'<td style="{style}; text-align: {alignment};">{row[col]}</td>'
        html += '</tr>'
    html += '</tbody></table>'

    # Add custom CSS for table styling
    custom_css = """
        <style>
        body {
            font-family: 'Calibri', sans-serif;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            border: none;
            font-family: 'Calibri', sans-serif;
        }
        th, td {
            border: none;
            padding: 3px;
            font-family: 'Calibri', sans-serif;
        }
        th {
            background-color: #f2f2f2;
            font-size: 12px;
            font-family: 'Calibri', sans-serif;
        }
        td {
            font-size: 10px;
            font-family: 'Calibri', sans-serif;
        }
        th:nth-child(1), td:nth-child(1),
        th:nth-child(4), td:nth-child(4),
        th:nth-child(7), td:nth-child(7),
        th:nth-child(11), td:nth-child(11) {
            border-left: 2px solid black;
        }
        th:nth-child(15), td:nth-child(15) {
            border-right: 2px solid black;
        }
        thead th {
            border-top: 2px solid black;
        }
        tbody tr:last-child td {
            border-bottom: 2px solid black;
        }
        /* Add specific column widths and overflow handling for certain columns */
        th, td {
            width: 100px; /* Default width for all columns */
            font-family: 'Calibri', sans-serif;
        }
        th:nth-child(2), td:nth-child(2),  /* Industry column */
        th:nth-child(3), td:nth-child(3) { /* Name column */
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        </style>
    """
    return custom_css + html


# Function to save styled DataFrame as HTML
def save_html(html, html_file):
    with open(html_file, 'w') as f:
        f.write(html)


# Function to convert HTML to PDF
def convert_html_to_pdf(html_file, pdf_file, wkhtmltopdf_path):
    config = pdf.configuration(wkhtmltopdf=wkhtmltopdf_path)
    pdf.from_file(html_file, pdf_file, configuration=config)


# Main function to run the workflow
def generate_table_html(api_key, rics, sector):
    fields = [
        'TR.PE.Date',
        'TR.ExchangeTicker',
        'CF_NAME',
        'TR.ICBIndustry',
        'TR.CompanyMarketCap',
        'CF_LAST',
        'TR.AvgDailyValTraded30D',
        'TR.TotalReturn1Wk',
        'TR.TotalReturn1Mo',
        'TR.TotalReturn3Mo',
        'TR.TotalReturnYTD',
        'TR.PriceToBVPerShare',
        'TR.DividendYield',
        'TR.PE',  # P/E LTM
        'TR.PriceClose(SDate=0D,Curn=USD)/TR.EPSMean(Period=NTM,SDate=0D,Curn=USD)',  # P/E NTM
        'TR.PriceClose(SDate=0D,Curn=USD)/TR.EPSMean(Period=FY1,SDate=0D,Curn=USD)'  # P/E FY1
    ]
    parameters = {'SDate': 0, 'EDate': 0, 'FRQ': 'D', 'Curn': 'USD'}
    csv_file = f'raw_data/table_{sector}.csv'

    if os.path.exists(csv_file):
        print(f"Loading data for sector {sector} from cache.")
        df = pd.read_csv(csv_file)
    else:
        print(f"Fetching data for sector {sector} from Eikon.")
        setup_eikon_api(api_key)
        df = fetch_data(rics, fields, parameters)
        df = preprocess_dataframe(df)
        df.to_csv(csv_file, index=False)

    styled_df = style_dataframe(df)
    html_file = f'htmls/styled_dataframe_{sector}.html'
    save_html(styled_df, html_file)
    return html_file


if __name__ == "__main__":
    api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
    rics = ['GOOG.O', 'AAPL.O', 'MSFT.O', 'TSLA.O']
    generate_table_html(api_key, rics)
