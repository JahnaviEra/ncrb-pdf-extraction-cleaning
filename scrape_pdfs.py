"""
Script to scrape and download PDFs from the NCRB website for Accidental Deaths & Suicides in India reports.
The script extracts PDF links, organizes them into folders by year and category, and downloads them concurrently.
"""
import os
import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Tuple
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor


# Setup logging
logging.basicConfig(
    filename="ncrb_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Base URL
BASE_URL_TEMPLATE = "https://ncrb.gov.in/accidental-deaths-suicides-in-india-table-content.html?year={year}&category="

OUTPUT_FOLDER = "all_ncrb_pdfs"

# Create the main folder if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def format_folder_name(folder_name:str) -> str:
    """
    Formats folder names by extracting the text after '--' and replacing spaces with underscores.
    
    Args:
    folder_name (str): The raw folder name to format.
    
    Returns:
    str: The formatted folder name.
    """
    # Extract text after '--' if it exists
    match = re.search(r'--\s*(.+)', folder_name)
    if match:
        folder_name = match.group(1).strip()
    return folder_name.replace(" ", "_")

def clean_filename(file_name: str) -> str:
    """
    Cleans the PDF filename by removing invalid characters and leading numbers.
    
    Args:
    file_name (str): The raw file name to clean.
    
    Returns:
    str: The cleaned file name.
    """
    # Remove any leading numbers or special patterns like 
    file_name = re.sub(r'^[0-9A-Z\.]+_', '', file_name)
    
    # Remove invalid characters and replace spaces with underscores
    return re.sub(r'[^a-zA-Z0-9 \-_]', '', file_name).strip().replace(' ', '_')

def download_pdf(pdf_url: str, file_name: str, folder_path: str) -> None:
    """
    Downloads a PDF file from the given URL and saves it to the specified folder.
    
    Args:
    pdf_url (str): The URL of the PDF file.
    file_name (str): The name of the PDF file to save.
    folder_path (str): The path to the folder where the file should be saved.
    """
    pdf_path = os.path.join(folder_path, file_name)
    
    try:
        pdf_response = requests.get(pdf_url, stream=True,timeout=60)
        if pdf_response.status_code == 200:
            with open(pdf_path, "wb") as pdf_file:
                for chunk in pdf_response.iter_content(chunk_size=1024):
                    pdf_file.write(chunk)
            logging.info(f"Downloaded: {file_name} in {folder_path}")
        else:
            logging.warning(f"Failed to download: {pdf_url}")
    except Exception as e:
        logging.error(f"Error downloading {pdf_url}: {e}")

def extract_pdf_links_from_page(year: int) -> List[Tuple[str, str, str]]:
    """
    Extracts all the PDF links and folder names from the target webpage for a given year.
    
    Returns:
    list: A list of tuples containing PDF URL, file name, and the folder path to save the file.
    """
    base_url = BASE_URL_TEMPLATE.format(year=year)
    response = requests.get(base_url)
    if response.status_code != 200:
        logging.warning(f"Failed to fetch the webpage for year {year}")
        return []
    time.sleep(3)

    soup = BeautifulSoup(response.text, "html.parser")
    pdf_links = []

    # Loop through all the headings and extract PDF links
    for heading in soup.find_all("h2", class_="c-genriccontent__subhead"):
        folder_name = heading.text.strip()
        formatted_folder_name = format_folder_name(folder_name)
        folder_path = os.path.join(OUTPUT_FOLDER, str(year), formatted_folder_name)

        # Only create the folder if PDFs are found
        table = heading.find_next("table", class_="c-table")
        if not table:
            continue

        # Check if PDFs exist for this heading
        pdf_found = False

        for row in table.find_all("tr")[1:]:  # Skip header row
            number_cell = row.find("td", class_="w-10")
            name_cell = row.find("td", class_="w-70")
            link = row.find("a", href=True)

            if number_cell and name_cell and link:
                pdf_url = urljoin(base_url, link["href"])
                file_name = clean_filename(name_cell.text.strip()) + ".pdf"
                pdf_links.append((pdf_url, file_name, folder_path))
                pdf_found = True

        # Create the folder only if PDFs are found
        if pdf_found:
            os.makedirs(folder_path, exist_ok=True)

    return pdf_links

def download_all_pdfs() -> None:
    """
    Downloads all PDF files extracted from the target webpage for all years (1950-2022) concurrently.
    """
    start_time = time.time()
    # Loop through years {1950 to 2022}
    for year in range(1950, 2023):
        logging.info(f"Processing year {year}...")
        pdf_links = extract_pdf_links_from_page(year)

        if pdf_links:
            # Use ThreadPoolExecutor to download PDFs concurrently
            with ThreadPoolExecutor(max_workers=1) as executor:
                for pdf_url, file_name, folder_path in pdf_links:
                    executor.submit(download_pdf, pdf_url, file_name, folder_path)
        else:
            logging.info(f"No PDFs found for year {year}. Skipping...")
    logging.info(f"\nTotal time taken: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    download_all_pdfs()
