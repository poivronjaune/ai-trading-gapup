import pandas as pd
import sys
import os

def identify_gap_ups(ticker, input_folder='DATA', output_folder='DATAGAP'):
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Load the CSV file
    input_path = os.path.join(input_folder, f"{ticker}.csv")
    df = pd.read_csv(input_path, index_col='datetime', parse_dates=True)
    
    # Calculate previous close
    df['Prev_Close'] = df['Close'].shift(1)
    
    # Identify gap up
    df['GAPUP'] = df['Open'] > df['Prev_Close']
    
    # Calculate gap percent
    df['GAPPERCENT'] = (df['Open'] - df['Prev_Close']) / df['Prev_Close'] * 100
    
    # Set GAPPERCENT to 0 where no gap up
    df.loc[~df['GAPUP'], 'GAPPERCENT'] = 0
    
    # Drop the temporary Prev_Close column
    df.drop('Prev_Close', axis=1, inplace=True)
    
    # Save to new CSV
    output_path = os.path.join(output_folder, f"{ticker}-GAP.csv")
    df.to_csv(output_path)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python gap_ups.py <ticker>")
        sys.exit(1)
    
    ticker = sys.argv[1]
    identify_gap_ups(ticker)

    