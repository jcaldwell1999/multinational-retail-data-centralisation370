import pandas as pd

class DataCleaner:
    """
    Class for cleaning extracted data.
    """

    def clean_user_data(self, user_df):
        """
        Cleans user data by handling missing values, formatting issues,
        and removing errors.
        """
        print("\n Checking for missing values before cleaning:")
        print(user_df.isnull().sum())

        # Convert date columns to datetime format
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'], errors="coerce")
        user_df['join_date'] = pd.to_datetime(user_df['join_date'], errors='coerce')

        # Standardize phone numbers
        user_df['phone_number'] = user_df['phone_number'].astype(str).str.replace(r'\D', '', regex=True)

        # Remove duplicate rocords
        user_df = user_df.drop_duplicates()

        # Remove invalid birthdays
        valid_dob = (user_df['date_of_birth'] > '1900-01-01') & (user_df['date_of_birth'] < '2025-02-20')
        user_df = user_df[valid_dob]

        print("Data cleaning complete.")
        return user_df
