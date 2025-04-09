from database_utils import DatabaseConnector
from sqlalchemy import text

"""
def alter_orders_table_schema():
    connector = DatabaseConnector()
    engine = connector.init_db_engine()

    alter_query = text(
    ALTER TABLE orders_table
        ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID,
        ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
        ALTER COLUMN card_number SET DATA TYPE VARCHAR(19),
        ALTER COLUMN store_code SET DATA TYPE VARCHAR(12),
        ALTER COLUMN product_code SET DATA TYPE VARCHAR(11),
        ALTER COLUMN product_quantity SET DATA TYPE SMALLINT;
    )    

    with engine.connect() as connection:
        connection.execute(alter_query)
        print("orders_table schema updated successfully.")
"""
if __name__ == "__main__":
#    alter_orders_table_schema()
    None