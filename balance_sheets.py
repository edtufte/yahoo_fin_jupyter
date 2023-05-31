import yahoo_fin.stock_info as si
import pandas as pd
import numpy as np
import concurrent.futures
import time

def pull_fin_info(function_call, stock_list):
    """
    Fetches financial information using the provided function call for each stock in the stock list concurrently.

    Args:
        function_call (function): A function to fetch financial information for a given stock.
        stock_list (list): List of stock tickers.

    Returns:
        pandas.DataFrame: DataFrame containing the fetched financial information for each stock.
    """
    futures_df = {}  # Dictionary to store futures for each stock ticker
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit function_call as a separate process for each stock ticker
        for ticker in stock_list:
            futures_df[ticker] = executor.submit(function_call, ticker)

    results_df = {}  # Dictionary to store results of function_call for each stock ticker
    for ticker in stock_list:
        try:
            results_df[ticker] = futures_df[ticker].result(60)  # Retrieve the result with a timeout of 60 seconds
        except:
            print('pull error on ' + ticker)
            break

    recent_sheets = pd.DataFrame(np.empty((0, 1)))  # Empty DataFrame to store recent financial sheets
    recent_sheets = {ticker: sheet.iloc[:, :1] for ticker, sheet in results_df.items()}  # Extract only the first column of each sheet
    for ticker in recent_sheets.keys():
        try:
            recent_sheets[ticker].columns = ['Recent']  # Rename the column as 'Recent'
        except ValueError:
            print('column re-name error on ' + ticker)

    recent_sheets = pd.concat(recent_sheets)  # Concatenate the individual sheets into one DataFrame
    recent_sheets = recent_sheets.reset_index()  # Reset the index
    recent_sheets.columns = ['Ticker', 'Breakdown', 'Recent']  # Rename the columns
    return recent_sheets


def main_program():
    startTime = time.time()
    print('timer started seconds - ' + str(round(time.time() - startTime, 2)))

    stock_list = ['AMZN', 'KO', 'TSLA', 'GME', 'AAPL', 'GOOG', 'SPOT']
    stock_list.extend(['ELV'])  # Extend the stock_list with additional tickers

    stock_list.extend(si.tickers_dow())  # Extend the stock_list with tickers from the Dow Jones Industrial Average
    # stock_list.extend(si.tickers_sp500())  # Extend the stock_list with tickers from the S&P 500 index (currently commented out)
    print('stock list pull done seconds - ' + str(round(time.time() - startTime, 2)))

    stock_list = list(dict.fromkeys(stock_list))  # Remove duplicates from the stock_list
    # print(stock_list)

    balance_sheets = pull_fin_info(si.get_balance_sheet, stock_list)  # Fetch balance sheets for the stock_list
    print('balance sheets done seconds - ' + str(round(time.time() - startTime, 2)))

    income_statements = pull_fin_info(si.get_income_statement, stock_list)  # Fetch income statements for the stock_list
    print('income statements done seconds - ' + str(round(time.time() - startTime, 2)))

    cash_flow_statements = pull_fin_info(si.get_cash_flow, stock_list)  # Fetch cash flow statements for the stock_list
    print('cash flow statements done seconds - ' + str(round(time.time() - startTime, 2)))

    frames = [balance_sheets, income_statements, cash_flow_statements]  # Combine the fetched dataframes
    combined_df = pd.concat(frames)  # Concatenate the dataframes
    combined_df = combined_df.reset_index()  # Reset the index

    pivot_df = combined_df.pivot_table(index='Ticker', columns='Breakdown', values='Recent', aggfunc='sum')  # Create a pivot table

    print(pivot_df.query("Ticker == 'AMZN'"))  # Print the pivot table filtered for a specific ticker

    print(pivot_df.query("ebit >= 0"))  # Print the pivot table filtered for a specific column (e.g., ebit >= 0)

    print(pivot_df.head())  # Print the first few rows of the pivot table

    print('Execution time in seconds: ' + str(round(time.time() - startTime, 2)))  # Print the total execution time


if __name__ == '__main__':
    main_program()
