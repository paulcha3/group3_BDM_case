import os
import pandas as pd

def convert_to_csv(df):
    """Save DataFrame to CSV in the NLP subdirectory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_dir = os.path.join(script_dir, 'convert_to_csv')
    
    # Create directory if it doesn't exist
    os.makedirs(csv_dir, exist_ok=True)
    
    csv_path = os.path.join(csv_dir, 'summary.csv')
    df.to_csv(csv_path, index=False)
    
    return csv_path