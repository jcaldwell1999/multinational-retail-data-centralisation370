# Multinational Retail Data Centralisation

## Project Overview
This project involves the extraction, transformation and loating of raw retail data from multiple formats (CSV, PDF, JSON, API, S3, and PostreSQL RDS).
The goal is to centralise the data into a structured PostgreSQL database for easier analysis.

### What I've learned:
- How to clean and normalise real-world messy data
- Writing modular and maintainable Python code
- Using APIs, AWS S3, SQL, and pandas

## Installation
1. Clone repository
```bash
git clone https://github.com/jcaldwell1999/multinational-retail-data-centralisation370.git
```
2. Create virtual environment
```bash
conda create -n mrdc python=3.10
conda activate mrdc
```
3. Install dependencies
```bash
pip install -r requirements.txt
```

## Usage
- Run the pipeline:
```bash
python main.py
```

### Folder structure
MULTINATIONAL-RETAIL-DATA-CENTRALISATION370/
├── __pycache__/                  # Compiled Python cache files (auto-generated)
├── .venv/                        # Python virtual environment
├── csvs/                         # Folder for any downloaded or working CSVs
├── .gitignore                    # Git ignore file for excluded files/folders
├── card_details.pdf             # PDF file used for extracting card data
├── data_cleaning.py             # Contains all data cleaning functions and logic
├── data_extraction.py           # Contains methods for extracting data from sources (API, PDF, S3, DB)
├── database_utils.py            # Utility class for connecting to and interacting with the PostgreSQL database
├── db_creds.yaml                # Database credentials for AWS RDS (excluded from GitHub via .gitignore)
├── main.py                      # Main script to run and orchestrate the data pipeline
├── product_df.csv               # Sample extracted CSV for products 
└── README.md                    # Project documentation

## License

This project is for educational purposes as part of the AiCore curriculum.