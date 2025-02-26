from typing import Annotated
from dotenv import load_dotenv
import os
import threading
import requests, json, httpx
import xlwings as xw
from fastapi import Depends, FastAPI, status, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.engine import URL
import pandas as pd

app = FastAPI()

XLWINGS_LICENSE_KEY="noncommercial"
XLWINGS_SECRET_KEY="aIHarFw2u3pbnmkF_PABfWrwGqgm2Rd_vFBq8zrm5AU="
# # Load .env file..
# load_dotenv(override=True)  # Forces `.env` variables to overwrite existing ones.

# # Access environment variables
# sql_server = os.getenv("SQL_SERVER")
# sql_database = os.getenv("SQL_DATABASE")
# sql_user = os.getenv("SQL_USER")
# sql_password = os.getenv("SQL_PASSWORD")
# sql_port = os.getenv("SQL_PORT")
# secret_key = os.getenv("XLWINGS_SECRET_KEY")

# # CONNECTION_STRING = f"mssql+pyodbc://{sql_user}:{sql_password}@{sql_server}/{sql_database}?driver=ODBC+Driver+17+for+SQL+Server"

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

async def get_db_engine(book: Book, bulk: bool = True) -> Engine:
    # Retrieve settings from the workbook.
    settings = await get_book_settings(book)
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

@app.post("/get/book_settings")
async def get_book_settings(book: Book):
    b_settings = book.sheets["Settings"]
    
    # Extract keys from column A (starting at A2) and values from column B (starting at B2).
    keys = b_settings.range("A2").expand("down").value
    values = b_settings.range("B2").expand("down").value
    
    # Combine the keys and values into a dictionary.
    settings = dict(zip(keys, values))
    # print(settings)
    return settings 

@app.post("/get/sheet_settings")
async def get_sheet_settings(book: Book):
    s_settings = book.sheets.active
    # Extract keys from column A (starting at A2) and values from column B (starting at B2).
    keys = s_settings.range("A1").expand("down").value
    values = s_settings.range("B1").expand("down").value
    
    # Combine the keys and values into a dictionary.
    settings = dict(zip(keys, values))
    # print(settings)
    return settings 




@app.post("/get/journals")
async def get_journals(book: Book):
    settings = await get_book_settings(book)
    db_schema = settings.get("DatabaseSchema")
    db_view = settings.get("DatabaseVW_TB_Journals")
    tb_date = settings.get("TB_Date")
    
    try:
        # Get the database engine using the book dependency.
        engine = await get_db_engine(book)
        with engine.connect() as connection:
            query = f"SELECT JournalLineID, JournalNumber, JournalDate, SourceType, Description, Reference, Contact, NetAmount, Mapping, Offset, JrnlURL FROM {db_schema}.{db_view} WHERE JournalDate <= '{tb_date}'"
            df = pd.read_sql(query, connection)

        # Write the DataFrame to the "Journals" sheet starting at cell A1.
        active_sheet = book.sheets["data"]
        active_sheet.clear_contents()
        active_sheet['A1'].value = df

        # Return the book's JSON representation or any other response as needed.
        return book.json()

    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
        
@app.post("/get/journals/sheet")
async def get_journals_sheet(book: Book):
    try:
        settings = await get_book_settings(book)
        db_schema = settings.get("DatabaseSchema")
        db_view = settings.get("DatabaseVW_TB_Journals")
        tb_date = settings.get("TB_Date")
        
        sheet_settings = await get_sheet_settings(book)
        account_id = sheet_settings.get("account_id")
        
        # Get the database engine using the book dependency.
        engine = await get_db_engine(book)
        with engine.connect() as connection:
            query = f"""
                SELECT JournalLineID, JournalNumber, JournalDate, SourceType, Description, Reference, Contact, NetAmount, Mapping, Offset, JrnlURL 
                FROM {db_schema}.{db_view} 
                WHERE AccountID = '{account_id}' AND JournalDate <= '{tb_date}'
            """
            df = pd.read_sql(query, connection)
            df.sort_values(by=["JournalDate","JournalNumber"], ascending=True, inplace=True)

        # Write the DataFrame to the "Journals" sheet starting at the specified cell.
        active_sheet = book.sheets.active
        
        input_cell = sheet_settings.get("input_cell")  # Default to A1 if input_cell is not specified
        
        # clear the range from input cell, to the right and down, only up to column S, and not column T.
        active_sheet[header_cell].expand("right").expand("down").clear_contents()
        
        active_sheet[input_cell].options(index=False, header=False).value = df

        # Return the book's JSON representation or any other response as needed.
        return book.json()

    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@app.post("/get/journals_offset")
async def get_journals(book: Book):
    settings = await get_book_settings(book)
    db_schema = settings.get("DatabaseSchema")
    db_view = settings.get("DatabaseVW_TB_Journals_Offset")
    tb_date = settings.get("TB_Date")
    
    try:
        # Get the database engine using the book dependency.
        engine = await get_db_engine(book)
        with engine.connect() as connection:
            query = f"SELECT * FROM {db_schema}.{db_view} WHERE JournalDate <= '{tb_date}'"
            df = pd.read_sql(query, connection)
            df.sort_values(by=["ModifiedDate"], ascending=True, inplace=True)

        # Write the DataFrame to the "Journals" sheet starting at cell A1.
        active_sheet = book.sheets["data_offset"]
        active_sheet['A1'].value = df

        # Return the book's JSON representation or any other response as needed.
        return book.json()

    except Exception as e:
        return PlainTextResponse(
            f"Error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
             
@app.post("/update/mapping_journals")
async def update_mapping_journals(book: Book):
    """
    Reads Mapping/Offset columns from the sheet, then inserts or updates
    them into the 'mapping_journals' table using the columns found in sheet_settings.
    Only rows with a non-null value in Mapping or Offset are processed.
    If the JournalLineID already exists, it updates that record; otherwise, it inserts a new one.
    
    Note:
    - Offset in the DB is a binary field (1 or 0).
    - JournalLineID and Mapping are text fields.
    """
    try:
        # 1. Get high-level settings (schema, table name, etc.)
        settings = await get_book_settings(book)
        db_schema = settings.get("DatabaseSchema")
        db_table = settings.get("DatabaseMappingJournals")  # e.g. "mapping_journals"

        # 2. Get sheet-specific settings
        sheet_settings = await get_sheet_settings(book)
        header_cell = sheet_settings.get("header_cell")  # e.g. "A1" (where headers start)

        active_sheet = book.sheets.active

        # Expand the range: first to the right then down from header_cell
        data_range = active_sheet[header_cell].expand("right").expand("down").clear_contents()
        
        # Read data into a DataFrame, treating the first row as header and not using the first column as an index.
        df = data_range.options(pd.DataFrame, header=True, index=False).value
        
        # Debug: print the DataFrame columns to verify the headers
        print("DataFrame columns:", df.columns)
        
        # 3. Filter rows that have a non-null value in either Mapping or Offset.
        df = df[(df["Mapping"].notnull()) | (df["Offset"].notnull())]

        # 4. Keep only the columns needed for the upsert.
        df = df[["JournalLineID", "Mapping", "Offset"]]
        print("Filtered DataFrame head:", df.head())

        # Helper function to convert Offset value to binary (1 or 0)
        def convert_to_binary(val):
            if pd.isnull(val):
                return 0
            if isinstance(val, bool):
                return 1 if val else 0
            if isinstance(val, (int, float)):
                return 1 if val != 0 else 0
            if isinstance(val, str):
                return 1 if val.strip().lower() in ['1', 'true', 'yes'] else 0
            return 0

        # 5. Perform upsert in the DB table using JournalLineID as the key.
        engine = await get_db_engine(book)
        with engine.begin() as connection:
            for _, row in df.iterrows():
                journal_line_id = row["JournalLineID"]
                mapping_value = row["Mapping"]
                # Convert NaN mapping_value to an empty string
                if pd.isnull(mapping_value):
                    mapping_value = ""
                offset_value = convert_to_binary(row["Offset"])

                # Try to update the record if JournalLineID exists.
                update_sql = f"""
                    UPDATE {db_schema}.{db_table}
                       SET Mapping = :mapping,
                           Offset = :offset,
                           ModifiedDate = GETUTCDATE()
                     WHERE JournalLineID = :jlid
                """
                result = connection.execute(
                    text(update_sql),
                    {
                        "mapping": mapping_value,
                        "offset": offset_value,
                        "jlid": journal_line_id,
                    }
                )

                # If no rows were updated, then insert a new record.
                if result.rowcount == 0:
                    insert_sql = f"""
                        INSERT INTO {db_schema}.{db_table} 
                           (JournalLineID, Mapping, Offset, ModifiedDate)
                             VALUES (:jlid, :mapping, :offset, GETUTCDATE())
                    """
                    connection.execute(
                        text(insert_sql),
                        {
                            "jlid": journal_line_id,
                            "mapping": mapping_value,
                            "offset": offset_value,
                        }
                    )
             
        # return {"message": "Successfully inserted/updated mapping_journals data."}
    
    except Exception as e:
        return PlainTextResponse(
            f"Error updating mapping_journals: {e}",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )





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

@app.post("/sync/fivetran")
async def fivetran_start_sync(book: Book):
  
    try:
        settings = await get_book_settings(book)
        connector_id = settings.get("FivetranConnectorID")
        base64_key = settings.get("FivetranBase64APIkey")
    
        # Endpoint for triggering sync. (Ensure this matches Fivetran's docs.)
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/sync"
    
        payload = {"force": False}
    
        headers = {
            "Accept": "application/json;version=2",
            "Authorization": f"Basic {base64_key}",
            "Content-Type": "application/json"
        }
        
        
        # Synchronous GET request using the requests module.
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
    except requests.HTTPError as exc:
        print(f"Error fetching connector sync: {exc}")
        return {"error": str(exc)}
    
    response_data = response.json()
    print(response_data)
    
    # Extract the sync state.
    sync_state = response_data.get("code", "unknown")
    if sync_state == "Success":
        sync_state = "Syncing"
    else:
        sync_state = "Not Syncing"
  
    # Update cell H4 in the "Main_Summary" sheet.
    sheet = book.sheets["Main_Summary"]
    sheet["G4"].value = sync_state

    return book.json()


@app.post("/status/fivetran")
async def fivetran_status(book: Book):
    """
    Fetches the Fivetran connector details and returns only the sync status.
    """
    settings = await get_book_settings(book)
    connector_id = settings.get("FivetranConnectorID")
    base64_key = settings.get("FivetranBase64APIkey")  # Should be Base64 encoded "api_key:api_secret"
    
    # Use the connectors endpoint per the Fivetran API docs.
    url = f"https://api.fivetran.com/v1/connectors/{connector_id}"
    
    headers = {
        "Accept": "application/json;version=2",
        "Authorization": f"Basic {base64_key}"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()  # Raise for any HTTP errors.
    except httpx.HTTPError as exc:
        print(f"Error fetching connector state: {exc}")
        return {"error": str(exc)}
    
    # Extract the sync state from the JSON response.
    response_data = response.json()
    sync_state = response_data.get("data", {}).get("status", {}).get("sync_state", "unknown")
    if sync_state == "scheduled":
        sync_state = "Not Syncing"
    else:
        sync_state = "Syncing"
    
    print(sync_state)
    
    sheet = book.sheets["Main_Summary"]
    sheet["G4"].value = sync_state

    return book.json()

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