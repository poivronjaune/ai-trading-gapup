import pandas as pd
import sys
import os

def load_stock_data(ticker: str, input_folder: str = 'DATA') -> pd.DataFrame:
    """Load stock OHLCV data from a CSV file, ignoring the first numerical index column and Adj Close."""
    input_path = os.path.join(input_folder, f"{ticker}.csv")
    # Load CSV, parse 'Datetime' as dates, drop the first column (numerical index) and 'Adj Close'
    df = pd.read_csv(input_path, parse_dates=['Datetime'])
    df = df.drop(columns=[df.columns[0], 'Adj Close']).set_index('Datetime')
    return df

def calculate_previous_close(df: pd.DataFrame) -> pd.DataFrame:
    """Add a column for the previous day's last closing price for the first candle of each day."""
    # Extract date from Datetime index
    df['Date'] = df.index.date
    # Get the last close of each day
    last_close = df.groupby('Date')['Close'].last().shift(1)
    # Map previous day's close to the first candle of each day
    df['Prev_Day_Close'] = None
    first_candles = df.groupby('Date').first()
    for date, close in last_close.items():
        if date in first_candles.index:
            df.loc[df['Date'] == date, 'Prev_Day_Close'] = close
    return df

def detect_gap_ups(df: pd.DataFrame) -> pd.DataFrame:
    """Identify gap ups for the first candle of each day where open > previous day's close."""
    # Initialize GAPUP as False
    df['GAPUP'] = False
    # Set GAPUP to True for first candles where Open > Prev_Day_Close
    first_candles_idx = df.groupby('Date').first().index
    df.loc[(df['Date'].isin(first_candles_idx)) & (df['Open'] > df['Prev_Day_Close']), 'GAPUP'] = True
    return df

def calculate_gap_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate gap percentage, set non-gap rows to 0, and label gap sizes for first candles."""
    # Calculate gap percentage
    df['GAPPERCENT'] = (df['Open'] - df['Prev_Day_Close']) / df['Prev_Day_Close'] * 100
    df.loc[~df['GAPUP'], 'GAPPERCENT'] = 0
    
    # Initialize GAPSIZE column
    df['GAPSIZE'] = 'None'
    
    # Apply gap size labels based on thresholds for gap-up candles
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] < 5), 'GAPSIZE'] = 'Small'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 5) & (df['GAPPERCENT'] < 7), 'GAPSIZE'] = 'Large'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 7) & (df['GAPPERCENT'] < 10), 'GAPSIZE'] = 'Moderate'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 10) & (df['GAPPERCENT'] < 20), 'GAPSIZE'] = 'Aggressive'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 20), 'GAPSIZE'] = 'Extreme'
    
    return df

def clean_temporary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove temporary columns used in calculations."""
    return df.drop(['Prev_Day_Close', 'Date'], axis=1)

def save_gap_data(df: pd.DataFrame, ticker: str, output_folder: str = 'DATAGAP') -> None:
    """Save the processed DataFrame to a CSV file in the output folder."""
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, f"{ticker}-GAP.csv")
    df.to_csv(output_path)

def identify_gap_ups(ticker: str, input_folder: str = 'DATA', output_folder: str = 'DATAGAP') -> None:
    """Process stock data to identify daily gap ups and save results."""
    try:
        # Load and process data through modular steps
        df = load_stock_data(ticker, input_folder)
        df = calculate_previous_close(df)
        df = detect_gap_ups(df)
        df = calculate_gap_percentage(df)
        df = clean_temporary_columns(df)
        save_gap_data(df, ticker, output_folder)
        print(f"Successfully processed and saved gap data for {ticker}")
    except FileNotFoundError:
        print(f"Error: {ticker}.csv not found in {input_folder}")
    except KeyError as e:
        print(f"Error: Required column not found in {ticker}.csv - {str(e)}")
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python gap_ups.py <ticker>")
        sys.exit(1)
    
    ticker = sys.argv[1]
    identify_gap_ups(ticker)