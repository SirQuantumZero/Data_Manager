# FILE: src/core/data/fetch_modules/fetch_polygon_data.py

import os
import argparse
from polygon import RESTClient
import pandas as pd
from dotenv import load_dotenv
import mysql.connector


def log_debug_message(cursor, message):
    cursor.execute("INSERT INTO DebugLogs (message) VALUES (%s)", (message,))
    print(message)  # Also print the message for immediate feedback


def fetch_polygon_data(data_type, ticker, start_date, end_date):
    # Load environment variables from .env file
    load_dotenv()

    # Get the Polygon API key from environment variable
    polygon_api_key = os.getenv("POLYGON_API_KEY")

    if not polygon_api_key:
        raise ValueError(
            "Polygon API key not found. Please set the POLYGON_API_KEY environment variable."
        )

    # Get MySQL database credentials from environment variables
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_database = os.getenv("DB_DATABASE")

    # Connect to MySQL database
    db_connection = mysql.connector.connect(
        host=db_host, user=db_user, password=db_password, database=db_database
    )
    cursor = db_connection.cursor()

    # Log debug message
    log_debug_message(cursor, "Connected to the database")

    # Initialize the Polygon REST client
    client = RESTClient(polygon_api_key)

    # Fetch data from Polygon based on the data type
    try:
        if data_type == "stocks":
            response = client.get_aggs(ticker, 1, "day", start_date, end_date)
            table_name = "PolygonData"
        elif data_type == "options":
            response = client.get_aggs(ticker, 1, "day", start_date, end_date)
            table_name = "OptionsData"
        elif data_type == "forex":
            response = client.get_aggs(ticker, 1, "day", start_date, end_date)
            table_name = "ForexData"
        elif data_type == "crypto":
            response = client.get_aggs(ticker, 1, "day", start_date, end_date)
            table_name = "CryptoData"
        else:
            raise ValueError(
                "Unsupported data type. Supported types are: stocks, options, forex, crypto."
            )

        data = response

        # Convert the data to a DataFrame
        df = pd.DataFrame(data)

        # Insert data into the appropriate table
        for _, row in df.iterrows():
            cursor.execute(
                f"""
                INSERT INTO {table_name} (
                    open, high, low, close, volume, vwap, timestamp, transactions, otc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row["vwap"],
                    row["timestamp"],
                    row["transactions"],
                    row["otc"],
                ),
            )

        # Commit and close the database connection
        db_connection.commit()
        cursor.close()
        db_connection.close()

        print(
            f"Polygon data for {ticker} from {start_date} to {end_date} inserted into the {table_name} table"
        )
    except Exception as e:
        log_debug_message(cursor, f"Error fetching data from Polygon: {e}")
        db_connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch data from Polygon API and insert into MySQL database."
    )
    parser.add_argument(
        "--data_type",
        type=str,
        required=True,
        help="Type of data to fetch (stocks, options, forex, crypto)",
    )
    parser.add_argument(
        "--ticker", type=str, required=True, help="Ticker symbol to fetch data for"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        required=True,
        help="Start date for data fetching (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        required=True,
        help="End date for data fetching (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    fetch_polygon_data(args.data_type, args.ticker, args.start_date, args.end_date)
