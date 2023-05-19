# %%
import pandas as pd


def generate_parquets(file1_df, file2_df):
    new_df1, new_df2  = file1_df.copy(), file1_df.copy()
    new_df1['earnings'] = file1_df['earnings'] + file2_df['earnings']
    new_df2['earnings'] = abs(file1_df['earnings'] - file2_df['earnings']) * 10 # Random formula

    new_df1.to_parquet('./data/output_data/employee_earnings/earnings_date=2022-02-15/employee_earnings.parquet')
    new_df2.to_parquet('./data/output_data/employee_earnings/earnings_date=2022-02-16/employee_earnings.parquet')
    return 

# Load Data
pd.read_parquet('./data/output_data/employee_earnings/earnings_date=2022-02-13/employee_earnings.parquet') \
    .to_csv('./data/file1.csv')
pd.read_parquet('./data/output_data/employee_earnings/earnings_date=2022-02-14/employee_earnings.parquet') \
    .to_csv('./data/file2.csv')
 
file1_df = pd.read_csv('./data/file1.csv')
file2_df = pd.read_csv('./data/file2.csv')
generate_parquets(file1_df, file2_df)



