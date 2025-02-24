from typing import Annotated
from dotenv import load_dotenv
import os
import threading

import xlwings as xw
from fastapi import Depends, FastAPI, status, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from sqlalchemy import create_engine, Engine
from sqlalchemy.engine import URL
import pandas as pd



app = FastAPI()

# Load .env file..
load_dotenv(override=True)  # Forces `.env` variables to overwrite existing ones.

# Access environment variables
sql_server = os.getenv("SQL_SERVER")
sql_database = os.getenv("SQL_DATABASE")
sql_user = os.getenv("SQL_USER")
sql_password = os.getenv("SQL_PASSWORD")
sql_port = os.getenv("SQL_PORT")
secret_key = os.getenv("XLWINGS_SECRET_KEY")

# CONNECTION_STRING = f"mssql+pyodbc://{sql_user}:{sql_password}@{sql_server}/{sql_database}?driver=ODBC+Driver+17+for+SQL+Server"

# This is the type annotation that we're using in the endpoint

# OLD
# def get_db_engine(bulk: bool = True) -> Engine:
#     settings = await get_settings(book)
#     try:
#         con_str = URL.create(
#             "mssql+pyodbc",
#             username=sql_user,
#             password=sql_password,
#             host=sql_server,
#             port=sql_port,
#             database=sql_database,
#             query={"driver": "ODBC Driver 17 for SQL Server"}
#         )
#         return create_engine(con_str, echo=True)
#     except Exception as e:
#         raise RuntimeError(f"Failed to create SQLAlchemy engine: {e}")


def get_book(body: dict):
    """Dependency that returns the calling book and cleans it up again."""
    book = xw.Book(json=body)
    try:
        yield book
    finally:
        book.close()
        
Book = Annotated[xw.Book, Depends(get_book)]


@app.post("/settings")
async def get_settings(book: Book):
    settings_sheet = book.sheets["Settings"]
    
    # Extract keys from column A (starting at A2) and values from column B (starting at B2).
    keys = settings_sheet.range("A2").expand("down").value
    values = settings_sheet.range("B2").expand("down").value
    
    # Combine the keys and values into a dictionary.
    settings = dict(zip(keys, values))
    print(settings)
    return settings 


async def get_db_engine(book: Book, bulk: bool = True) -> Engine:
    # Retrieve settings from the workbook.
    settings = await get_settings(book)
    try:
        con_str = URL.create(
            "mssql+pyodbc",
            username=settings.get("DatabaseUsername"),
            password=settings.get("DatabasePassword"),
            host=settings.get("DatabaseHost"),
            port=settings.get("DatabasePort"),
            database=settings.get("DatabaseName"),
            query={"driver": "ODBC Driver 17 for SQL Server"}
        )
        return create_engine(con_str, echo=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create SQLAlchemy engine: {e}")


@app.post("/get/journals")
async def get_journals(book: Book):
    settings = await get_settings(book)
    db_schema = settings.get("DatabaseSchema")
    db_view = settings.get("DatabaseVW_TB_Journals")
    tb_date = settings.get("TB_Date")
    
    try:
        # Get the database engine using the book dependency.
        engine = await get_db_engine(book)
        with engine.connect() as connection:
            query = f"SELECT * FROM {db_schema}.{db_view} WHERE JournalDate <= '{tb_date}'"
            df = pd.read_sql(query, connection)

        # Write the DataFrame to the "Journals" sheet starting at cell A1.
        active_sheet = book.sheets["data"]
        active_sheet['A1'].value = df

        # Return the book's JSON representation or any other response as needed.
        return book.json()

    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
        
        
@app.post("/get/journals_offset")
async def get_journals(book: Book):
    settings = await get_settings(book)
    db_schema = settings.get("DatabaseSchema")
    db_view = settings.get("DatabaseVW_TB_Journals_Offset")
    tb_date = settings.get("TB_Date")
    
    try:
        # Get the database engine using the book dependency.
        engine = await get_db_engine(book)
        with engine.connect() as connection:
            query = f"SELECT * FROM {db_schema}.{db_view} WHERE JournalDate <= '{tb_date}'"
            df = pd.read_sql(query, connection)

        # Write the DataFrame to the "Journals" sheet starting at cell A1.
        active_sheet = book.sheets["data_offset"]
        active_sheet['A1'].value = df

        # Return the book's JSON representation or any other response as needed.
        return book.json()

    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
             

@app.post("/hello")
async def hello(book: Book):
    """If you're using FastAPI < 0.95.0, you have to replace the function signature
    like so: async def hello(book: xw.Book = Depends(get_book))
    """
    sheet = book.sheets[0]
    cell = sheet["A1"]
    if cell.value == "Hello xlwings!":
        cell.value = "Suck it, xlwings!"
    else:
        cell.value = "Hello xlwings!"

    # Return the following response
    return book.json()

@app.post("/yellow")
async def gs_yellow(book: Book):
    """
    Highlights the currently selected cells in Excel in yellow.
    """
    try:
        # Get the active sheet
        sheet = book.sheets.active

        # Get the currently selected range
        selected_range = book.app.selection

        # Set the background color to yellow (RGB: 255, 255, 0)
        selected_range.color = "#FFFF00"

        print("Selected cells highlighted in yellow!")
        
    except Exception as e:
        print(f"Error: {e}")

    # Return the following response
    return book.json()

@app.post("/upload_data")
async def upload_data(book: Book):
    """
    Endpoint to upload data from an Excel table to Azure SQL.
    """
    try:
        # Get data from the first sheet
        sheet = book.sheets[0]
        df = sheet["A1"].expand().options(pd.DataFrame).value

        # Validate DataFrame
        if df.empty:
            return {"message": "No data found in the sheet to upload"}

        # Save data to Azure SQL
        engine = get_db_engine()
        with engine.connect() as connection:
            df.to_sql("your_table_name", connection, if_exists="replace", index=False)

        return {"message": "Data uploaded successfully!"}
    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@app.post("/upsert_azure")       
def upsert_to_azure(df, table_name, primary_key):
    """
    Upserts a pandas DataFrame to an Azure SQL Database using a MERGE statement.

    :param df: pandas DataFrame to upsert
    :param table_name: Target table in Azure SQL Database
    :param primary_key: The primary key column of the target table
    """
    engine = get_engine_db()

    with engine.connect() as connection:
        # Step 1: Write DataFrame to a temporary table
        temp_table_name = "temp_table"
        df.to_sql(temp_table_name, connection, if_exists="replace", index=False)

        # Step 2: Perform the MERGE operation
        merge_query = f"""
        MERGE INTO {table_name} AS target
        USING (SELECT * FROM {temp_table_name}) AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET 
                {', '.join([f"{col} = source.{col}" for col in df.columns if col != primary_key])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(df.columns)})
            VALUES ({', '.join([f"source.{col}" for col in df.columns])});
        """

        # Execute the MERGE query
        connection.execute(text(merge_query))

        # Step 3: Drop the temporary table
        drop_temp_table_query = f"DROP TABLE {temp_table_name}"
        connection.execute(text(drop_temp_table_query))

    print(f"Upsert operation completed successfully on table '{table_name}'.")

# Example usage

@app.post("/clear")
async def clear_data(book: Book):
    try:
        # Access the active sheet
        active_sheet = book.sheets.active
        
        # Debugging statements
        print("Book object:", book)
        print("Active sheet:", active_sheet.name)

        # Validate the range and clear it
        range_to_clear = active_sheet.range("A5:ZZ1000")
        print("Range to clear:", range_to_clear.address)
        range_to_clear.clear_contents()
        
        print("Range cleared successfully.")
        return book.json()

    except Exception as e:
        # Detailed error message for debugging
        error_message = f"Error clearing data: {e}"
        print(error_message)
        return PlainTextResponse(error_message, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    


@app.post("/select_range")
async def download_data(book: Book):
    active_sheet = book.sheets.active
    print(active_sheet.range)

@app.post("/sheet_to_sql")
async def download_data(book: Book):
    try:
        
        # Fetch data from Azure SQL
        engine = get_db_engine()
        with engine.connect() as connection:
            query = "SELECT * FROM xero_jointfinances.vw_accounts"
            df = pd.read_sql(query, connection)

        # Convert DataFrame to dictionary for API response
        data = df.to_dict(orient="records")
        
        active_sheet = book.sheets.active
        active_sheet['A5'].value = df
        return book.json()

    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
        
@app.post("/sql_to_table")
async def download_data(book: Book):
    try:
        # Fetch data from Azure SQL
        engine = get_db_engine()
        with engine.connect() as connection:
            query = "SELECT * FROM xero_jointfinances.vw_accounts"
            df = pd.read_sql(query, connection)

        # Validate DataFrame
        if df.empty:
            raise ValueError("DataFrame is empty. Cannot populate Google Sheets table.")

        # Define range parameters
        start_row = 4  # Zero-based row for cell A5 (row 5 in human terms)
        start_column = 0  # Zero-based column for cell A5 (column A in human terms)
        row_count = df.shape[0]
        column_count = df.shape[1]

        # Create payload for Google Apps Script
        action = {
            "func": "addTable",
            "args": ["$A$5", True, "TableStyleMedium2", "DataTable"],
            "values": df.values.tolist(),  # Convert DataFrame to list of lists
            "sheet_position": book.sheets.active.index,
            "start_row": start_row,
            "start_column": start_column,
            "row_count": row_count,
            "column_count": column_count,
        }

        print("Payload:", action)  # Debugging log

        # Pass the payload to Google Apps Script
        return book.json()

    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )   
        
        
@app.exception_handler(Exception)
async def exception_handler(request, exception):
    # Handle all exceptions
    return PlainTextResponse(
        str(exception), status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )

# Office Scripts and custom functions in Excel on the web require CORS
cors_app = CORSMiddleware(
    app=app,
    allow_origins="*",
    allow_methods=["POST","GET"],
    allow_headers=["*"],
    allow_credentials=True,
)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:cors_app", host="0.0.0.0", port=7999, workers=2, reload=True)
    # settings = get_settings_dict()
    # print(settings)