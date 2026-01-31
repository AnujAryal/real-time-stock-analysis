"""
Test Script: Fetch Stock Data
This script tests our setup by fetching real stock data from Yahoo Finance
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
import os

def fetch_stock_data(symbol="AAPL", period="5d"):
    """
    Fetch stock data for a given symbol
    
    Args:
        symbol (str): Stock ticker symbol (default: AAPL)
        period (str): Time period to fetch (default: 5d for 5 days)
    
    Returns:
        pandas.DataFrame: Stock data
    """
    print(f"Fetching data for {symbol}...")
    
    try:
        # Create a Ticker object
        stock = yf.Ticker(symbol)
        
        # Get historical data with retry
        data = stock.history(period=period)
        
        # Alternative method if first fails
        if data.empty:
            print("Retrying with download method...")
            data = yf.download(symbol, period=period, progress=False)
        
        if data.empty:
            print(f"❌ No data found for {symbol}")
            return None
        
        # Add symbol column
        data['Symbol'] = symbol
        
        print(f"✅ Successfully fetched {len(data)} records")
        return data
        
    except Exception as e:
        print(f"❌ Error fetching data: {str(e)}")
        return None


def display_latest_info(data, symbol):
    """Display the latest stock information"""
    if data is None or data.empty:
        return
    
    latest = data.iloc[-1]
    
    print(f"\n📊 Latest Data for {symbol}:")
    print(f"   Date: {latest.name.strftime('%Y-%m-%d')}")
    print(f"   Open: ${latest['Open']:.2f}")
    print(f"   High: ${latest['High']:.2f}")
    print(f"   Low: ${latest['Low']:.2f}")
    print(f"   Close: ${latest['Close']:.2f}")
    print(f"   Volume: {latest['Volume']:,}")


def save_to_csv(data, symbol):
    """Save data to CSV file"""
    if data is None or data.empty:
        return
    
    # Create data/raw directory if it doesn't exist
    os.makedirs('data/raw', exist_ok=True)
    
    # Generate filename with today's date
    today = datetime.now().strftime('%Y-%m-%d')
    filename = f"data/raw/{symbol}_{today}.csv"
    
    # Save to CSV
    data.to_csv(filename)
    print(f"\n💾 Data saved to: {filename}")


def main():
    """Main function"""
    print("=" * 50)
    print("Stock Data Fetch Test")
    print("=" * 50)
    
    # Test with Apple stock
    symbol = "AAPL"
    data = fetch_stock_data(symbol, period="5d")
    
    if data is not None:
        display_latest_info(data, symbol)
        save_to_csv(data, symbol)
        
        print("\n" + "=" * 50)
        print("✅ TEST SUCCESSFUL!")
        print("=" * 50)
        print("\nYour setup is working correctly!")
        print("Next steps:")
        print("1. Check the data/raw/ folder for the CSV file")
        print("2. Ready to build the Airflow pipeline!")
    else:
        print("\n❌ TEST FAILED!")
        print("Please check your internet connection and try again.")


if __name__ == "__main__":
    main()
