import pandas as pd
import re

class DataCleaner:
    """
    Class for cleaning extracted data.
    """

    def parse_dates(self, date_str):
        """
        Attempts to parse a date string into a uniform YYYY-MM-DD format.

        :param date_str: The date string to be converted.
        :return: Converted date in standard datetime format or NaT if invalid.
        """
        if pd.isnull(date_str) or date_str.strip() == "":
            return pd.NaT  # Empty values should return NaT

        try:
            return pd.to_datetime(date_str, infer_datetime_format=True)  # Try automatic parsing
        except Exception:
            pass 

        possible_formats = [
            "%Y-%m-%d",    # 1993-03-29
            "%B %d, %Y",   # October 26, 2022
            "%d %B %Y",    # 26 October 2022
            "%B %Y %d"     # October 2022 26
        ]

        for fmt in possible_formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue  # Try next format
        
        return pd.NaT  # Return NaT (Not a Time) if no format works

    def clean_user_data(self, user_df):
        """
        Cleans user data by handling missing values, formatting issues,
        and removing errors.

        :param user_df: DataFrame containing raw user data.
        :return: Cleaned DataFrame.
        """

        print("\n Checking for missing values before cleaning:")
        print(user_df.isnull().sum())

        # Convert date columns using the new date parsing function
        user_df['date_of_birth'] = user_df['date_of_birth'].apply(self.parse_dates)
        user_df['join_date'] = user_df['join_date'].apply(self.parse_dates)

        # Remove rows with invalid date_of_birth or join_date
        user_df = user_df.dropna(subset=['date_of_birth', 'join_date'])

        # Standardize phone numbers (remove non-numeric characters)
        user_df['phone_number'] = user_df['phone_number'].astype(str).str.replace(r'\D', '', regex=True)

        # Remove duplicate records
        user_df = user_df.drop_duplicates()

        # Remove invalid birthdays (e.g., unrealistic values)
        valid_dob = (user_df['date_of_birth'] > '1900-01-01') & (user_df['date_of_birth'] < '2025-02-20')
        user_df = user_df[valid_dob]

        print("Data cleaning complete.")
        return user_df

    def clean_card_data(self, card_df):
        """
        Cleans card details by removing NULL values, duplicates, and ensuring correct formatting.

        :param card_df: DataFrame containing raw card details.
        :return: Cleaned DataFrame.
        """
        print("\nChecking missing values before cleaning: ")
        print(card_df.isnull().sum())

        # Remove duplicate rows
        card_df = card_df.drop_duplicates()

        # Convert date columns if needed
        if 'expiry_date' in card_df.columns:
            card_df.loc[:, 'expiry_date'] = pd.to_datetime(card_df['expiry_date'], format='%m/%y', errors='coerce')

        # Replace NULL with empty string
        card_df['card_number'] = card_df['card_number'].fillna("").astype(str)

        # Allow non-numeric characters, but keep the numbers
        card_df.loc[:, 'card_number'] = card_df['card_number'].apply(lambda x: re.sub(r'\D', '', x))

        # Drop rows where BOTH 'card_number' and 'expiry_date' are NULL
        card_df = card_df.dropna(subset=['card_number', 'expiry_date'])

        # Drop any completely empty rows
        #card_df = card_df.dropna(how='all')

        print("Data cleaning complete. Preview of cleaned data: ")
        print(card_df.head())

        print("\nNo. of unique card numbers: ")
        print(card_df['card_number'].unique())

        return card_df