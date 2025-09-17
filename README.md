# ai-trading-gapup
Simple python function to open a OHLCV csv file with minute price data to identify price GAPS going up. Output file is a copy of the data with 2 new columns
> [!WARNING]
> This code **DOES NOT USE** Ai models when run. The code was generated using vibecoding techniques. 
> 

# NO WORKING
Gaps are identified using a simple logic of (current day's open at 9:30) - (previous' day close at 15:59)  
Code uses the first and last of each day, sorted on the datetime for evry minute.  
Gaps are idnetified but they are not what was expected. :(  
