"""
This script processes Incidence and Rate of Suicides(State/UT & City - wise) related
PDF files, extracts table data, and saves it as cleaned CSV files.

Features:
- Recursively searches for PDFs in directories with suicide-related keywords.
- Extracts tables from valid PDFs using Camelot.
- Cleans and processes extracted data (removing unwanted rows, renaming columns, and handling missing values).
- Saves processed data into CSV files for state-wise and city-wise suicide statistics.
- Cleans and refines the saved CSV files before final storage.

Usage:
    python script.py <folder_path>
"""
import os
import re
import time
import camelot
import argparse
import pandas as pd
from typing import Optional, Tuple

OUTPUT_STATE_FILE='state_data.csv'
OUTPUT_CITY_FILE='city_data.csv'
OUTPUT_FOLDER = "cleaned_data"

#folder name keywords
FOLDER_KEYWORDS = ["suicides", "suicides in india", "suicide data", "suicide report"]

#keywords in PDF filenames
REQUIRED_TERMS = ["Incidence", "Rate", "State", "Suicides", "City", "wise"]

def is_valid_folder(folder_path:str)->bool:
    """Check if any part of the folder path contains the required keywords (ignoring case)."""
    folder_parts = folder_path.lower().replace("_", " ").split(os.sep)  # Normalize folder path
    return any(keyword in part for part in folder_parts for keyword in FOLDER_KEYWORDS)

def is_valid_pdf(filename:str)->bool:
    """Check if a PDF filename contains all required terms."""
    clean_filename = filename.replace("_", " ").lower()  # Normalize filename
    return all(term.lower() in clean_filename for term in REQUIRED_TERMS)

def find_suicide_data_pdfs(base_folder:str) -> list:
    """Search for PDFs in valid 'suicide-related' folders across multiple levels."""
    matching_pdfs = []
    
    for root, _, files in os.walk(base_folder):
        # Check if any part of the path (year folder, suicide folder, etc.) is relevant
        if is_valid_folder(root):  
            for file in files:
                if file.endswith('.pdf') and is_valid_pdf(file):
                    matching_pdfs.append(os.path.join(root, file))
    
    return matching_pdfs

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to lowercase and snake_case."""
    df.columns = [re.sub(r'\s+', '_', str(col).strip().lower()) for col in df.columns]
    return df

def add_year_column(df: pd.DataFrame, pdf_filename: str) -> pd.DataFrame:
    """Extract year from PDF filename and add it as a column."""
    match = re.search(r'(\d{4})', pdf_filename)
    year = match.group(1) if match else None  
    df['year'] = year
    return df

def filter_valid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows where the first column starts with a numeric value."""
    if df.empty or df.shape[1] < 2:  # Ensure dataframe has data and at least two columns
        return df
    
    df = df[df.iloc[:, 0].astype(str).str.match(r'^\d+$')]  # Keep rows where the first column is numeric
    return df.reset_index(drop=True)

def extract_and_process_pdf(pdf_file: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Extract tables from a valid PDF and process them."""
    tables = camelot.read_pdf(pdf_file, pages='1,2', flavor='stream', edge_tol=500)

    if not tables or len(tables) == 0:
        print(f"No tables found in {pdf_file}")
        return None, None
    
    state_df = clean_column_names(tables[0].df)
    city_df = clean_column_names(tables[1].df) if len(tables) > 1 else None

    state_df = add_year_column(state_df, pdf_file)
    state_df = filter_valid_rows(state_df) 

    if city_df is not None:
        city_df = add_year_column(city_df, pdf_file)
        city_df = filter_valid_rows(city_df)  
    
    return state_df, city_df

def process_pdfs(base_folder: str) -> None:
    """Find and extract data from suicide-related PDFs."""
    pdf_files = find_suicide_data_pdfs(base_folder)
    
    all_state_data = pd.DataFrame()
    all_city_data = pd.DataFrame()

    for pdf_file in pdf_files:
        state_df, city_df = extract_and_process_pdf(pdf_file)
        
        if state_df is not None and not state_df.empty:
            all_state_data = pd.concat([all_state_data, state_df], ignore_index=True)
        if city_df is not None and not city_df.empty:
            all_city_data = pd.concat([all_city_data, city_df], ignore_index=True)
        
        print(f"Processed: {pdf_file}")

    # Save final cleaned data
    if not all_state_data.empty:
        all_state_data.to_csv(OUTPUT_STATE_FILE, index=False)
        print("Saved: combined_state_data.csv")

    if not all_city_data.empty:
        all_city_data.to_csv(OUTPUT_CITY_FILE, index=False)
        print("Saved: combined_city_data.csv")


def clean_and_save_csv(csv_filename: str, column_names: list[str], threshold: float = 0.5) -> Optional[str]:
    """Read a CSV file, clean unnecessary columns, drop rows with too many NaN values, and save the cleaned data."""
    try:
        #Check file existence
        if not os.path.exists(csv_filename):
            print(f"{csv_filename} does not exist.")
            return None
        
        # Load the CSV into a DataFrame
        print(f"Reading file: {csv_filename}")
        df = pd.read_csv(csv_filename)

        # Check if the file has content
        if df.empty:
            print(f"{csv_filename} is empty or has no data.")
            return None
    
        # Drop columns that are completely NaN or empty
        df = df.dropna(axis=1, how='all')  # Drop columns with all NaN values
        df = df.loc[:, ~df.isin(['', None]).all()]  # Remove columns with all empty strings or None
        
        # Remove any extra columns after the 'Year' column
        if 'year' in df.columns:
            year_index = df.columns.get_loc('year')
            df = df.iloc[:, :year_index + 1]  # Keep columns up to and including 'Year'

        # Drop rows with too many NaN values (if more than the threshold of NaNs in a row)
        nan_threshold = len(df.columns) * threshold
        df = df.dropna(thresh=len(df.columns) - nan_threshold)
        
        # Drop rows where more than two columns are NaN or empty
        df = df[df.isna().sum(axis=1) <= 2]  # Drop rows where more than 2 columns are NaN
        
        # Drop rows where all columns are NaN or empty (check for empty strings as well)
        df = df[~df.apply(lambda row: row.astype(str).eq('').all(), axis=1)] 

        
        # Check if the dataframe has the expected number of columns
        if len(df.columns) == len(column_names):
            df.columns = column_names
        else:
            print(f"Column mismatch in {csv_filename}. Data has {len(df.columns)} columns, but expected {len(column_names)}.")
            return None
        
        if "Sl. No." in df.columns:
            df = df.drop(columns=["Sl. No."])

        # Check if any data remains after cleaning
        if df.empty:
            print(f"After cleaning, {csv_filename} has no valid data.")
            return None
        
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)  # Create the folder if it does not exist

        # Save the cleaned DataFrame in the folder
        cleaned_filename = os.path.join(OUTPUT_FOLDER, f"cleaned_{csv_filename}")
        df.to_csv(cleaned_filename, index=False)
        print(f"Saved cleaned file: {cleaned_filename}")
        os.remove(csv_filename)
        print(f"Deleted original file: {csv_filename}")

        return cleaned_filename
        
    except Exception as e:
        print(f"Error processing {csv_filename}: {e}")
        return None


def process_and_clean_csv_files()-> None:
    """Process all the saved CSV files and clean them."""
    # Column names for state and city data
    column_names_state = [
        "Sl. No.", "State/UT", "Number of Suicides", 
        "Percentage Share in Total Suicides", 
        "Estimated Mid–Year Population (in Lakh)", 
        "Rate of Suicides (Col.3/Col.5)", 
        "Rank for State/UT", "Year"
    ]

    column_names_city = [
        "Sl. No.", "Cities", "Number of Suicides", 
        "Percentage Share in Total Suicides", 
        "Estimated Mid–Year Population (in Lakh)", 
        "Rate of Suicides (Col.3/Col.5)", 
        "Rank for Cities", "Year"
    ]
    
    csv_files = [OUTPUT_STATE_FILE, OUTPUT_CITY_FILE]
    
    for csv_file in csv_files:
        if 'state' in csv_file:
            clean_and_save_csv(csv_file, column_names_state)
        else:
            clean_and_save_csv(csv_file, column_names_city)


def main()-> None:
    """Main function to process PDFs and CSV files."""
    #argument parser
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Process suicide data PDFs.")
    parser.add_argument('folder', type=str, help="Path to the folder containing the PDFs.")
    
    # Parse arguments
    args = parser.parse_args()
    folder_path = args.folder  # Get the folder path from command line
    
    process_pdfs(folder_path)
    process_and_clean_csv_files()
    print(f"\nTotal time taken: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
