import pandas as pd
import sys
import os

def load_stock_data(ticker: str, input_folder: str = 'DATA') -> pd.DataFrame:
    """Load stock OHLCV data from a CSV file, ignoring the first numerical index column and Adj Close, sort by Datetime, and remove duplicates."""
    input_path = os.path.join(input_folder, f"{ticker}.csv")
    # Load CSV, parse 'Datetime' as dates, drop the first column (numerical index) and 'Adj Close'
    df = pd.read_csv(input_path, parse_dates=['Datetime'])
    df = df.drop(columns=[df.columns[0], 'Adj Close']).set_index('Datetime')
    # Sort by Datetime index to ensure chronological order
    df = df.sort_index()
    # Remove duplicate rows based on index
    df = df[~df.index.duplicated(keep='first')]
    return df

def calculate_previous_close(df: pd.DataFrame) -> pd.DataFrame:
    """Add columns for the previous day's last closing price and high, only for the first candle of each day."""
    # Extract date from Datetime index
    df['Date'] = df.index.date
    # Get the last close and high of each day, shifted by 1 for previous day
    last_close = df.groupby('Date')['Close'].last().shift(1)
    last_high = df.groupby('Date')['High'].last().shift(1)
    # Calculate 20-day average volume for first candles
    first_candles = df.groupby('Date').head(1)
    avg_volume = first_candles['Volume'].rolling(window=20, min_periods=1).mean().shift(1)
    # Initialize columns as NaN
    df['Prev_Day_Close'] = float('nan')
    df['Prev_Day_High'] = float('nan')
    df['Volume_Avg'] = float('nan')
    # Assign previous day's close, high, and avg volume to the first candle of each day
    for date in df['Date'].unique():
        first_idx = df[df['Date'] == date].index.min()
        if first_idx is not None:
            if date in last_close.index:
                df.loc[first_idx, 'Prev_Day_Close'] = last_close[date]
            if date in last_high.index:
                df.loc[first_idx, 'Prev_Day_High'] = last_high[date]
            if date in avg_volume.index:
                df.loc[first_idx, 'Volume_Avg'] = avg_volume[date]
    return df

def detect_gap_ups(df: pd.DataFrame) -> pd.DataFrame:
    """Identify gap ups for the first candle of each day where open > previous day's last close, with volume and size filters."""
    # Initialize GAPUP as False
    df['GAPUP'] = False
    # Get the Datetime indices of the first candles per day
    first_candles_idx = df.groupby('Date').head(1).index
    # Valid comparison: first candles, Open > Prev_Day_Close, gap >= 1%, volume > avg, and no NaN
    valid_comparison = (
        df.index.isin(first_candles_idx) &
        (df['Open'] > df['Prev_Day_Close']) &
        (~df['Prev_Day_Close'].isna()) &
        ((df['Open'] - df['Prev_Day_Close']) / df['Prev_Day_Close'] * 100 >= 1.0) &  # Min 1% gap
        (df['Volume'] > df['Volume_Avg']) &  # Volume > 20-day avg
        (~df['Volume_Avg'].isna())
    )
    df.loc[valid_comparison, 'GAPUP'] = True
    return df

def calculate_gap_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate gap percentage, set non-gap rows to 0, and label gap sizes and types."""
    # Calculate gap percentage (will be NaN where Prev_Day_Close is NaN)
    df['GAPPERCENT'] = (df['Open'] - df['Prev_Day_Close']) / df['Prev_Day_Close'] * 100
    df.loc[~df['GAPUP'], 'GAPPERCENT'] = 0
    
    # Initialize GAPSIZE and GAPTYPE columns
    df['GAPSIZE'] = 'None'
    df['GAPTYPE'] = 'None'
    
    # Apply gap size labels for gap-up candles
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] < 5), 'GAPSIZE'] = 'Small'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 5) & (df['GAPPERCENT'] < 7), 'GAPSIZE'] = 'Large'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 7) & (df['GAPPERCENT'] < 10), 'GAPSIZE'] = 'Moderate'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 10) & (df['GAPPERCENT'] < 20), 'GAPSIZE'] = 'Aggressive'
    df.loc[df['GAPUP'] & (df['GAPPERCENT'] >= 20), 'GAPSIZE'] = 'Extreme'
    
    # Label gap type (Full if Open > Prev_Day_High, Partial otherwise)
    df.loc[df['GAPUP'] & (df['Open'] > df['Prev_Day_High']) & (~df['Prev_Day_High'].isna()), 'GAPTYPE'] = 'Full'
    df.loc[df['GAPUP'] & (df['Open'] <= df['Prev_Day_High']) & (~df['Prev_Day_High'].isna()), 'GAPTYPE'] = 'Partial'
    
    return df

def clean_temporary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove temporary columns used in calculations."""
    return df.drop(['Prev_Day_Close', 'Prev_Day_High', 'Volume_Avg', 'Date'], axis=1)

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
        #df = clean_temporary_columns(df)
        save_gap_data(df, ticker, output_folder)
        print(f"Successfully processed and saved gap data for {ticker}")
        # Print number of gap ups detected
        gap_count = df['GAPUP'].sum()
        print(f"Detected {gap_count} gap ups")
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