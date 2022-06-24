import yahoo_fin.stock_info as si
import pandas as pd
import numpy as np
import concurrent.futures
import time

def pull_fin_info(function_call, stock_list):
    futures_df = {}
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for ticker in stock_list:
                futures_df[ticker] = executor.submit(function_call,ticker)

    results_df = {}
    for ticker in stock_list:
        try:
            results_df[ticker] = futures_df[ticker].result(12) # 60 sec timeout
            break
        except:
            print('error on ' + ticker)

    recent_sheets = pd.DataFrame(np.empty((0, 1)))
    recent_sheets = {ticker : sheet.iloc[:,:1] for ticker,sheet in results_df.items()}
    for ticker in recent_sheets.keys():
        recent_sheets[ticker].columns = ['Recent']
    
    recent_sheets = pd.concat(recent_sheets)
    recent_sheets = recent_sheets.reset_index()
    recent_sheets.columns = ['Ticker', 'Breakdown', 'Recent']
    return recent_sheets

startTime = time.time()

print('timer started seconds - ' + str(round(time.time() - startTime,2)))

stock_list = ['AMZN','KO','TSLA','GME','AAPL','GOOG','SPOT']

# stock_list.extend(si.tickers_ftse100())
# stock_list.extend(si.tickers_sp500())

stock_list = list(dict.fromkeys(stock_list))
print(stock_list)

print('stock list pull done seconds - ' + str(round(time.time() - startTime,2)))

balance_sheets = pull_fin_info(si.get_balance_sheet, stock_list)

print('balance sheets done seconds - ' + str(round(time.time() - startTime,2)))

income_statements = pull_fin_info(si.get_income_statement, stock_list)

print('income statements done seconds - ' + str(round(time.time() - startTime,2)))

cash_flow_statements = pull_fin_info(si.get_cash_flow, stock_list)

print('cash flow statements done seconds - ' + str(round(time.time() - startTime,2)))

frames = [balance_sheets, income_statements, cash_flow_statements]
combined_df = pd.concat(frames)
combined_df = combined_df.reset_index()

pivot_df = combined_df.pivot_table(index='Ticker', columns='Breakdown', values='Recent', aggfunc='sum')

print(pivot_df.query("Ticker == 'AMZN'"))

print(pivot_df.query("ebit >= 0"))

print('Execution time in seconds: ' + str(round(time.time() - startTime,2)))