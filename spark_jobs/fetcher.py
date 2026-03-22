import yfinance as yf
import time
import random

def fetch_ticker_data(ticker, period="1mo"):
    """
    In yfinance 1.1.0+, the library handles 'impersonate="chrome"' 
    internally using curl_cffi. We just need to call it directly.
    """
    # Still keep a small delay to avoid rapid-fire hits
    delay = random.uniform(1, 3)
    print(f"🕒 Humanizing delay: {delay:.2f}s...")
    time.sleep(delay)
    
    try:
        print(f"📡 Requesting {ticker} (using yfinance built-in impersonation)...")
        
        # DO NOT pass a 'session' argument anymore. 
        # yfinance will now automatically initialize a curl_cffi session.
        data = yf.download(ticker, period=period, progress=False)
        
        if data.empty:
            print(f"⚠️ No data returned for {ticker}. Check ticker symbol.")
            return None
            
        return data
        
    except Exception as e:
        print(f"❌ API Failure: {e}")
        return None