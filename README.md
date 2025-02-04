# ncrb-pdf-extraction-cleaning
This project automates scraping PDFs from the NCRB Accidental Deaths and Suicides in India reports for multiple years. Using Camelot, it extracts, cleans the data from pdfs.

# steps:

1. Create a Virtual Environment
2. Activate the Virtual Environment
3. Install Dependencies
Now, install the required dependencies using the requirements.txt file:
-------->  pip install -r requirements.txt   <---------------

## Task 1: Scrape the PDFs from NCRB Accidental Deaths & Suicides

# Introduction
This script scrapes the National Crime Records Bureau (NCRB) website for reports on accidental deaths and suicides in India.
The script extracts PDF links, organizes them into folders by year and category, and downloads the PDFs concurrently. 
It logs the progress and errors, ensuring efficient downloading.

# Features
Scrapes PDF Links: Extracts PDF URLs from the NCRB website for each year.
Organizes by Year and Category: Folders are created for each year, and subfolders for each category.
Concurrent Downloads: Downloads PDFs concurrently using a thread pool, improving efficiency.
Error Logging: Logs the status of each download attempt, along with any errors.

# Requirements
Python 3.11
Libraries:
requests
beautifulsoup4
logging
concurrent.futures

# Run the Script: 
Run the script to start the scraping and downloading process. It will save the PDFs in the all_ncrb_pdfs folder.

---------> python scrape_pdfs.py  <-------------------

# Logging:
Logs are stored in the ncrb_scraper.log file. You can track:
Successfully downloaded PDFs
Errors encountered during the download process
Warnings if the webpage couldn't be fetched or no PDFs were found

 ## Task 2:Clean and Combine the Incidence and Rate of Suicides Data

# Introduction:
This script processes multiple years of Incidence and Rate of Suicides data, 
extracting tables from PDFs and combining data across multiple years into one consolidated dataset.

# Features:
Recursive Search for PDFs: Searches for PDFs in directories containing suicide-related keywords.
Table Extraction: Extracts tables from valid PDFs using the Camelot library.
**Data Cleaning:**
    Renames columns to snake_case.
    Filters rows that are not relevant or contain missing data.
    Adds the year of the report based on the PDF filename.
    Removes unnecessary columns and rows with excessive NaN values.
    Saving Data: The processed data is saved into state-wise and city-wise CSV files.
# Requirements:
Python 3.11
Libraries:
    Camelot
    Pandas
    Argparse
    Regular expressions (re)
    OS
**pip install camelot-py[cv] pandas**

# Usage:
To run the script, use the following command in the terminal:

python script.py <folder_path>

--------->  EX: python script.py /path/to/your/pdf/folder  <-----------------
# Output:
State-wise CSV: A CSV file containing state-wise suicide data with relevant columns like the number of suicides, percentage share, population, suicide rate, and rank.
City-wise CSV: A CSV file containing city-wise suicide data with similar columns.
The cleaned data will be saved in a folder named cleaned_data.
