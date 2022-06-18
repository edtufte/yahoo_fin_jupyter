import yahoo_fin.stock_info as si
import pandas as pd
import numpy as np
import concurrent.futures
import time

def pull_fin_info(df, function_call, stock_list):
    if function_call == "get_balance_sheet":
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for ticker in stock_list:
                        df[ticker] = executor.submit(si.get_balance_sheet,ticker)
        for ticker in stock_list:
            try:
                df[ticker] = df[ticker].result(60) # 60 sec timeout
            except:
                print('error on ' + ticker)

    if function_call == "get_income_statement":
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for ticker in stock_list:
                        df[ticker] = executor.submit(si.get_balance_sheet,ticker)
        for ticker in stock_list:
            try:
                df[ticker] = df[ticker].result(60) # 60 sec timeout
            except:
                print('error on ' + ticker)

    if function_call == "get_cash_flow":
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for ticker in stock_list:
                        df[ticker] = executor.submit(si.get_cash_flow,ticker)
        for ticker in stock_list:
            try:
                df[ticker] = df[ticker].result(60) # 60 sec timeout
            except:
                print('error on ' + ticker)

    recent_sheets = pd.DataFrame(np.empty((0, 1)))
    recent_sheets = {ticker : sheet.iloc[:,:1] for ticker,sheet in balance_sheets.items()}
    for ticker in recent_sheets.keys():
        recent_sheets[ticker].columns = ['Recent']
    return recent_sheets

startTime = time.time()
current_runtime = time.time() - startTime
print('timer started seconds - ' + str(round(current_runtime,2)))
balance_sheets = {}
stock_list =['AMZN','KO','TSLA','GME','AAPL','GOOG','SPOT']
stock_list.extend(si.tickers_sp500())
# print(stock_list)

current_runtime = time.time() - startTime
print('s&p500 pull done seconds - ' + str(round(current_runtime,2)))

balance_sheets = {}
balance_sheets = pull_fin_info(balance_sheets, "get_balance_sheet", stock_list)

current_runtime = time.time() - startTime 
print('balance sheets done seconds - ' + str(round(current_runtime,2)))

recent_sheets = pd.DataFrame(np.empty((0, 1)))   
recent_sheets = {ticker : sheet.iloc[:,:1] for ticker,sheet in balance_sheets.items()}

for ticker in recent_sheets.keys():
    recent_sheets[ticker].columns = ['Recent']
    
combined_sheets = pd.concat(recent_sheets)

combined_sheets = combined_sheets.reset_index()
combined_sheets.columns = ['Ticker', 'Breakdown', 'Recent']

current_runtime = time.time() - startTime 
print('combined sheets done seconds - ' + str(round(current_runtime,2)))

income_statements = {}
with concurrent.futures.ProcessPoolExecutor() as executor:
    for ticker in stock_list:
        income_statements[ticker] = executor.submit(si.get_income_statement,ticker)

for ticker in stock_list:
    income_statements[ticker] = income_statements[ticker].result()

current_runtime = time.time() - startTime 
print('income statements done seconds - ' + str(round(current_runtime,2)))

recent_income_statements = {ticker : sheet.iloc[:,:1] for ticker,sheet in income_statements.items()}
for ticker in recent_income_statements.keys():
    recent_income_statements[ticker].columns = ['Recent']

combined_income = pd.concat(recent_income_statements)
combined_income = combined_income.reset_index()
combined_income.columns = ['Ticker', 'Breakdown', 'Recent']

cash_flow_statements = {}
cash_flow_statements = pull_fin_info(cash_flow_statements, "get_cash_flow", stock_list)

current_runtime = time.time() - startTime
print('cash flow statements done seconds - ' + str(round(current_runtime,2)))

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

current_runtime = (time.time() - startTime)
print('Execution time in seconds: ' + str(round(current_runtime,2)))
