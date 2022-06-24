import yahoo_fin.stock_info as si
import pandas as pd
import numpy as np
import concurrent.futures
import time

def pull_fin_info(df, function_call, stock_list):

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for ticker in stock_list:
                df[ticker] = executor.submit(function_call,ticker)

    for ticker in stock_list:
        try:
            df[ticker] = df[ticker].result(12) # 60 sec timeout
            break
        except:
            print('error on ' + ticker)

    recent_sheets = pd.DataFrame(np.empty((0, 1)))
    recent_sheets = {ticker : sheet.iloc[:,:1] for ticker,sheet in df.items()}
    for ticker in recent_sheets.keys():
        recent_sheets[ticker].columns = ['Recent']
    
    recent_sheets = pd.concat(recent_sheets)
    recent_sheets = recent_sheets.reset_index()
    recent_sheets.columns = ['Ticker', 'Breakdown', 'Recent']
    return recent_sheets

startTime = time.time()
current_runtime = time.time() - startTime
print('timer started seconds - ' + str(round(current_runtime,2)))

balance_sheets = {}
stock_list = ['AMZN','KO','TSLA','GME','AAPL','GOOG','SPOT']

stock_list.extend(si.tickers_ftse100())
stock_list = list(dict.fromkeys(stock_list))
# print(stock_list)
current_runtime = time.time() - startTime
print('s&p500 pull done seconds - ' + str(round(current_runtime,2)))

balance_sheets = {}
balance_sheets = pull_fin_info(balance_sheets, si.get_balance_sheet, stock_list)

current_runtime = time.time() - startTime 
print('balance sheets done seconds - ' + str(round(current_runtime,2)))

income_statements = {}
income_statements = pull_fin_info(income_statements, si.get_income_statement, stock_list)

current_runtime = time.time() - startTime 
print('income statements done seconds - ' + str(round(current_runtime,2)))

cash_flow_statements = {}
cash_flow_statements = pull_fin_info(cash_flow_statements, si.get_cash_flow, stock_list)

current_runtime = time.time() - startTime
print('cash flow statements done seconds - ' + str(round(current_runtime,2)))

frames = [balance_sheets, income_statements, cash_flow_statements]
combined_df = pd.concat(frames)
combined_df = combined_df.reset_index()

pivot_df = combined_df.pivot_table(index='Ticker', columns='Breakdown', values='Recent', aggfunc='sum')

print(pivot_df.query("Ticker == 'AMZN'"))

print(pivot_df.query("ebit >= 0"))

current_runtime = (time.time() - startTime)
print('Execution time in seconds: ' + str(round(current_runtime,2)))

