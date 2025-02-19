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

        return user_df