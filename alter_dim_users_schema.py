from sqlalchemy import create_engine, text
from database_utils import DatabaseConnector
import yaml

# Set up SQLAlchemy engine
connector = DatabaseConnector()
engine = connector.init_db_engine()

def alter_dim_users_table_schema():
    with engine.connect() as connection:
        alter_query = text("""
            ALTER TABLE dim_users
                ALTER COLUMN first_name SET DATA TYPE VARCHAR(255),
                ALTER COLUMN last_name SET DATA TYPE VARCHAR(255),
                ALTER COLUMN date_of_birth SET DATA TYPE DATE USING date_of_birth::DATE,
                ALTER COLUMN country_code SET DATA TYPE VARCHAR(3),
                ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
                ALTER COLUMN join_date SET DATA TYPE DATE USING join_date::DATE;
        """)
        connection.execute(alter_query)
        print("Schema updated successfully for dim_users table.")

if __name__ == "__main__":
    alter_dim_users_table_schema()
