
import yahoo_fin.stock_info as si
import pandas as pd
import numpy as np
import concurrent.futures
import time

startTime = time.time()
balance_sheets = {}
stock_list =['AMZN','KO','TSLA','GME','AAPL','GOOG','SPOT']
stock_list.extend(si.tickers_sp500())
print(stock_list)

income_statements = {}
with concurrent.futures.ProcessPoolExecutor() as executor:
    for ticker in stock_list:
        balance_sheets[ticker] = executor.submit(si.get_balance_sheet,ticker)

for ticker in stock_list:
    try:
        balance_sheets[ticker] = balance_sheets[ticker].result(60) # 60 sec timeout
    except:
        print('error on ' + ticker)
    
print('balance sheets done - ' + str(time.time() - startTime))

recent_sheets = pd.DataFrame(np.empty((0, 1)))   
recent_sheets = {ticker : sheet.iloc[:,:1] for ticker,sheet in balance_sheets.items()}

for ticker in recent_sheets.keys():
    recent_sheets[ticker].columns = ['Recent']
    
combined_sheets = pd.concat(recent_sheets)

combined_sheets = combined_sheets.reset_index()
combined_sheets.columns = ['Ticker', 'Breakdown', 'Recent']

print('combined sheets done - ' + str(time.time() - startTime))

income_statements = {}
with concurrent.futures.ProcessPoolExecutor() as executor:
    for ticker in stock_list:
        income_statements[ticker] = executor.submit(si.get_income_statement,ticker)

for ticker in stock_list:
    income_statements[ticker] = income_statements[ticker].result()

print('income statements done - ' + str(time.time() - startTime))

recent_income_statements = {ticker : sheet.iloc[:,:1] for ticker,sheet in income_statements.items()}
for ticker in recent_income_statements.keys():
    recent_income_statements[ticker].columns = ['Recent']

combined_income = pd.concat(recent_income_statements)
combined_income = combined_income.reset_index()
combined_income.columns = ['Ticker', 'Breakdown', 'Recent']

cash_flow_statements = {}
with concurrent.futures.ProcessPoolExecutor() as executor:
    for ticker in stock_list:
        cash_flow_statements[ticker] = executor.submit(si.get_cash_flow,ticker)

for ticker in stock_list:
    cash_flow_statements[ticker] = cash_flow_statements[ticker].result()

print('cash flow statements done - ' + str(time.time() - startTime))

recent_cash_flow_statements = {ticker : sheet.iloc[:,:1] for ticker,sheet in income_statements.items()}
for ticker in recent_cash_flow_statements.keys():
    recent_cash_flow_statements[ticker].columns = ['Recent']

combined_cash_flow = pd.concat(recent_cash_flow_statements)
combined_cash_flow = combined_cash_flow.reset_index()
combined_cash_flow.columns = ['Ticker', 'Breakdown', 'Recent']

frames = [combined_sheets, combined_income, combined_cash_flow]
combined_df = pd.concat(frames)
combined_df = combined_df.reset_index()

pivot_df = combined_df.pivot_table(index='Ticker', columns='Breakdown', values='Recent', aggfunc='sum')

pivot_df.query("Ticker == 'AMZN'")

pivot_df.query("ebit >= 0")

executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(executionTime))
