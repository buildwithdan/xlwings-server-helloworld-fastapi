from typing import Annotated
from dotenv import load_dotenv
import os

import xlwings as xw
from fastapi import Depends, FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from sqlalchemy import create_engine, Engine
from sqlalchemy.engine import URL
import pandas as pd


app = FastAPI()

# Load .env file
load_dotenv(override=True)  # Forces `.env` variables to overwrite existing ones.

# Access environment variables
sql_server = os.getenv("SQL_SERVER")
sql_database = os.getenv("SQL_DATABASE")
sql_user = os.getenv("SQL_USER")
sql_password = os.getenv("SQL_PASSWORD")
sql_port = os.getenv("SQL_PORT")
secret_key = os.getenv("XLWINGS_SECRET_KEY")

# CONNECTION_STRING = f"mssql+pyodbc://{sql_user}:{sql_password}@{sql_server}/{sql_database}?driver=ODBC+Driver+17+for+SQL+Server"

def get_db_engine(bulk: bool = True) -> Engine:
    try:
        con_str = URL.create(
            "mssql+pyodbc",
            username=sql_user,
            password=sql_password,
            host=sql_server,
            port=sql_port,
            database=sql_database,
            query={"driver": "ODBC Driver 17 for SQL Server"}
        )
        return create_engine(con_str, echo=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create SQLAlchemy engine: {e}")


def get_book(body: dict):
    """Dependency that returns the calling book and cleans it up again"""
    book = xw.Book(json=body)
    try:
        yield book
    finally:
        book.close()

# This is the type annotation that we're using in the endpoints
Book = Annotated[xw.Book, Depends(get_book)]


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
async def yellow(book: Book):
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

@app.post("/download_data")
async def download_data(book: Book):
    """
    Endpoint to download data from Azure SQL as a pandas DataFrame.
    """
    
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
    
    uvicorn.run("main:cors_app", host="127.0.0.1", port=8000, reload=True)
