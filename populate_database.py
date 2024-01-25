import pandas as pd
from sqlalchemy import create_engine

# Function to read and insert data from Spreadsheet 0
def process_spreadsheet_0(engine):
    spreadsheet_path = "path/to/spreadsheet_0.xlsx"  # Update with the actual path

    # Read the spreadsheet into a DataFrame
    df = pd.read_excel(spreadsheet_path)

    # Insert data into the SQLite database
    df.to_sql("shipping_data", engine, if_exists="replace", index=False)

# Function to read, combine, and insert data from Spreadsheets 1 and 2
def process_spreadsheet_1_2(engine):
    spreadsheet_1_path = "path/to/spreadsheet_1.xlsx"  # Update with the actual path
    spreadsheet_2_path = "path/to/spreadsheet_2.xlsx"  # Update with the actual path

    # Read the spreadsheets into DataFrames
    df1 = pd.read_excel(spreadsheet_1_path)
    df2 = pd.read_excel(spreadsheet_2_path)

    # Merge DataFrames on the shipping identifier
    merged_df = pd.merge(df1, df2, how="inner", on="shipping_identifier")

    # Iterate through the merged DataFrame and insert data into the SQLite database
    for _, row in merged_df.iterrows():
        product_name = row["product_name"]
        quantity = row["quantity"]
        origin = row["origin"]
        destination = row["destination"]

        # Construct a DataFrame for the current row
        current_row_df = pd.DataFrame({
            "product_name": [product_name],
            "quantity": [quantity],
            "origin": [origin],
            "destination": [destination]
        })

        # Insert the current row into the SQLite database
        current_row_df.to_sql("shipping_data", engine, if_exists="append", index=False)

# Main script
if __name__ == "__main__":
    # SQLite database connection
    db_path = "path/to/database.db"  # Update with the actual path
    engine = create_engine(f"sqlite:///{db_path}")

    # Process Spreadsheet 0
    process_spreadsheet_0(engine)

    # Process Spreadsheets 1 and 2
    process_spreadsheet_1_2(engine)

    print("Data has been successfully inserted into the database.")
