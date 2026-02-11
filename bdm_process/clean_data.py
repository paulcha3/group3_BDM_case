from currency_converter import CurrencyConverter
import pandas as pd
import datetime

def get_fallback_rates():
    """
    Conversion rates for 2023.
    The rates represent the EUR value of one unit of foreign currency.
    Source BCE: https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html
    """
    return {
        'USD': 0.93,
        'JPY': 0.0070,
        'GBP': 1.14,
        'CHF': 1.02,
        'SGD': 0.70,
        'HKD': 0.12,
        'CNY': 0.14,
        'KRW': 0.00073,
        'TWD': 0.03,
        'AED': 0.24
    }

def convert_prices_to_eur(df):
    """
    Convert prices to euros with strict validations.
    """
    c = CurrencyConverter()
    fallback_rates = get_fallback_rates()
    
    def convert_row(row):
        try:
            if pd.isna(row['price']) or row['price'] <= 0:
                return None
                
            price = float(row['price'])
            currency = row['currency']
            
            # If already in EUR, verify price is realistic
            if currency == 'EUR':
                return price if 1000 <= price <= 100000 else None
            
            # Conversion date
            conversion_date = pd.to_datetime(row['life_span_date']).date()
            
            try:
                # First try with CurrencyConverter
                converted_price = c.convert(price, currency, 'EUR', date=conversion_date)
                
                # Validate result
                if 1000 <= converted_price <= 100000:
                    # Additional check for max 10% variation
                    fallback_price = price * fallback_rates.get(currency, 0)
                    if abs(converted_price - fallback_price) / fallback_price < 0.1:
                        return converted_price
            except:
                pass
                
            # If we get here, use only fallback rate
            if currency in fallback_rates:
                converted_price = price * fallback_rates[currency]
                if 1000 <= converted_price <= 100000:
                    return converted_price
                    
            return None
                
        except Exception as e:
            print(f"Conversion error for {row['reference_code']} in {currency}: {e}")
            return None

    # Save original prices
    df['original_price'] = df['price']
    df['original_currency'] = df['currency']
    df['conversion_method'] = 'direct'  # For EUR
    
    df['price_eur'] = df.apply(convert_row, axis=1)
    
    # Display statistics by currency
    print("\nConversion statistics by currency:")
    for currency in df['currency'].unique():
        currency_data = df[df['currency'] == currency]
        success_rate = (currency_data['price_eur'].notna().sum() / len(currency_data)) * 100
        print(f"{currency}: {success_rate:.1f}% success ({currency_data['price_eur'].notna().sum()}/{len(currency_data)})")
    
    return df

def clean_data(df):
    """
    Clean the dataset by applying basic filters and
    standardizing formats.
    """
    print("Starting data cleaning process...")
    
    df_clean = df.copy()
    initial_rows = len(df_clean)
    
    # 1. Cleaning collections
    print("1. Cleaning collections...")
    df_clean = df_clean[~df_clean['collection'].str.contains('HTTPS:', na=False)]
    
    # 2. Standardizing fields
    print("2. Standardizing fields...")
    df_clean['currency'] = df_clean['currency'].str.strip().str.upper()
    df_clean['collection'] = df_clean['collection'].str.strip()
    df_clean['reference_code'] = df_clean['reference_code'].str.strip()
    
    # 3. Converting dates
    print("3. Converting dates...")
    df_clean['life_span_date'] = pd.to_datetime(df_clean['life_span_date'], errors='coerce')
    
    # 4. Removing critical missing values
    print("4. Handling missing values...")
    df_clean = df_clean.dropna(subset=['price', 'collection', 'reference_code', 'life_span_date'])
    
    # 5. Cleaning prices
    print("5. Cleaning prices...")
    df_clean = df_clean[df_clean['price'] > 0]
    df_clean = convert_prices_to_eur(df_clean)
    df_clean = df_clean.dropna(subset=['price_eur'])
    
    # 6. Temporal columns
    print("6. Adding time columns...")
    df_clean['year'] = df_clean['life_span_date'].dt.year
    df_clean['quarter'] = df_clean['life_span_date'].dt.quarter
    
    # 7. Removing unnecessary columns
    print("7. Removing unnecessary columns...")
    cols_to_drop = [
        'is_new', 'country', 'price_before',
        'price_changed', 'price_percent_change', 'price_difference'
    ]
    df_clean = df_clean.drop(columns=cols_to_drop, errors='ignore')
    
    # Summary
    final_rows = len(df_clean)
    rows_removed = initial_rows - final_rows
    print(f"\nCleaning completed!")
    print(f"Rows removed: {rows_removed} ({rows_removed/initial_rows*100:.1f}%)")
    print(f"Final dataset: {final_rows} rows")
    
    return df_clean
