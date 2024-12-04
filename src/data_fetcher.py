# FILE: src/core/data/data_fetcher.py

import pandas as pd
import requests
import logging
from bs4 import BeautifulSoup
from textblob import TextBlob
from dotenv import load_dotenv
import os
import mysql.connector
from polygon import RESTClient
from typing import List, Dict, Any
from .polygon_client import PolygonClient
from .database_client import DatabaseClient


class DataFetcher:
    """
    Fetches real-time data from various APIs, scrapes financial news, and processes data.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()
        self.polygon_client = PolygonClient()
        self.database_client = DatabaseClient()

    def log_debug_message(self, cursor, message):
        cursor.execute("INSERT INTO DebugLogs (message) VALUES (%s)", (message,))
        print(message)  # Also print the message for immediate feedback

    def fetch_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch real-time stock data for a given symbol.

        Args:
            symbol (str): The stock symbol to fetch data for.

        Returns:
            Dict[str, Any]: The real-time stock data.
        """
        api_url = f"https://api.example.com/stock/{symbol}"
        self.logger.info(f"Fetching stock data for symbol: {symbol}")
        response = requests.get(api_url)
        if response.status_code == 200:
            self.logger.info(f"Successfully fetched stock data for symbol: {symbol}")
            return response.json()
        else:
            self.logger.error(
                f"Failed to fetch stock data for symbol: {symbol}, status code: {response.status_code}"
            )
            return {"status": "error", "message": "Failed to fetch stock data"}

    def fetch_crypto_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch real-time cryptocurrency data for a given symbol.

        Args:
            symbol (str): The cryptocurrency symbol to fetch data for.

        Returns:
            Dict[str, Any]: The real-time cryptocurrency data.
        """
        api_url = f"https://api.example.com/crypto/{symbol}"
        self.logger.info(f"Fetching cryptocurrency data for symbol: {symbol}")
        response = requests.get(api_url)
        if response.status_code == 200:
            self.logger.info(
                f"Successfully fetched cryptocurrency data for symbol: {symbol}"
            )
            return response.json()
        else:
            self.logger.error(
                f"Failed to fetch cryptocurrency data for symbol: {symbol}, status code: {response.status_code}"
            )
            return {"status": "error", "message": "Failed to fetch cryptocurrency data"}

    def scrape_news(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrape financial news from a given URL.

        Args:
            url (str): The URL to scrape news from.

        Returns:
            List[Dict[str, Any]]: A list of news articles.
        """
        self.logger.info(f"Scraping news from URL: {url}")
        response = requests.get(url)
        if response.status_code == 200:
            self.logger.info(f"Successfully scraped news from URL: {url}")
            soup = BeautifulSoup(response.content, "html.parser")
            articles = []
            for item in soup.find_all("article"):
                title = item.find("h2").get_text()
                summary = item.find("p").get_text()
                articles.append({"title": title, "summary": summary})
            return articles
        else:
            self.logger.error(
                f"Failed to scrape news from URL: {url}, status code: {response.status_code}"
            )
            return []

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Perform sentiment analysis on a given text.

        Args:
            text (str): The text to analyze.

        Returns:
            Dict[str, Any]: The sentiment analysis results.
        """
        self.logger.info(f"Analyzing sentiment for text: {text}")
        analysis = TextBlob(text)
        sentiment = {
            "polarity": analysis.sentiment.polarity,
            "subjectivity": analysis.sentiment.subjectivity,
        }
        self.logger.info(f"Sentiment analysis results: {sentiment}")
        return sentiment

    def fetch_polygon_data(self, cursor, data_type, ticker, start_date, end_date):
        """
        Fetch data from the Polygon API and insert it into the database.
        """
        # Get the Polygon API key from environment variable
        polygon_api_key = os.getenv("POLYGON_API_KEY")

        if not polygon_api_key:
            raise ValueError(
                "Polygon API key not found. Please set the POLYGON_API_KEY environment variable."
            )

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

            print(
                f"Polygon data for {ticker} from {start_date} to {end_date} inserted into the {table_name} table"
            )
        except Exception as e:
            self.log_debug_message(cursor, f"Error fetching data from Polygon: {e}")

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the input DataFrame with categorical encoding"""
        if df.empty:
            return pd.DataFrame({"trade_outcome": []}, dtype=int)

        # Create a copy to avoid modifying original
        result_df = df.copy()

        # Handle timestamp conversion safely
        if "timestamp" in result_df.columns:
            result_df["timestamp"] = (
                pd.to_numeric(result_df["timestamp"], errors="coerce").fillna(0) / 1000
            )

        # Handle numeric values safely
        if "value" in result_df.columns:
            result_df["value"] = pd.to_numeric(
                result_df["value"], errors="coerce"
            ).fillna(0)

        # Add trade outcome column
        result_df["trade_outcome"] = 1

        # Process categorical columns safely
        categorical_columns = result_df.select_dtypes(include=["object"]).columns
        for col in categorical_columns:
            if col != "timestamp":
                # Convert numeric categories to strings
                result_df[col] = result_df[col].astype(str)
                # Get dummies for category column
                dummies = pd.get_dummies(result_df[col], prefix=col)
                # Drop original column
                result_df = result_df.drop(col, axis=1)
                # Join with dummies
                result_df = pd.concat([result_df, dummies], axis=1)

        return result_df

    def combine_data(self):
        """
        Combine data from different sources and insert it into the HistoricalData table.
        """
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
        self.log_debug_message(cursor, "Connected to the database")

        # Fetch data from PolygonData table
        cursor.execute("SELECT * FROM PolygonData")
        polygon_data = cursor.fetchall()
        polygon_columns = [desc[0] for desc in cursor.description]
        polygon_df = pd.DataFrame(polygon_data, columns=polygon_columns)

        # Combine data into a single DataFrame
        combined_df = polygon_df  # Assuming only Polygon data is used now

        # Insert combined data into HistoricalData table
        for _, row in combined_df.iterrows():
            cursor.execute(
                """
                INSERT INTO HistoricalData (
                    symbol, trade_type, order_type, trade_outcome,
                    realtime_start, realtime_end, date, fred_value,
                    units, start, cik, entity
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    "AAPL",
                    "BUY",
                    "market",
                    1,  # Placeholder values
                    row["realtime_start"],
                    row["realtime_end"],
                    row["date"],
                    row["fred_value"],
                    row["units"],
                    row["start"],
                    row["cik"],
                    row["entity"],
                ),
            )

        # Commit and close the database connection
        db_connection.commit()
        cursor.close()
        db_connection.close()

        print("Combined data inserted into HistoricalData table")

    def fetch_historical_data(self):
        """
        Fetch historical data from various sources and insert it into the database.
        """
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
        self.log_debug_message(cursor, "Connected to the database")

        # Fetch and insert data from Polygon API
        self.fetch_polygon_data(cursor, "stocks", "AAPL", "2022-01-01", "2022-12-31")

        # Commit and close the database connection
        db_connection.commit()
        cursor.close()
        db_connection.close()

        print("Historical data fetched and inserted into the database")

    def fetch_market_data(self, symbol: str, start_date: str, end_date: str):
        """Fetch market data from Polygon API"""
        try:
            client = PolygonClient()  # Or pass API key if needed
            return client.get_market_data(symbol, start_date, end_date)
        except Exception as e:
            raise Exception(f"Market data fetch failed: {e}")

    def fetch_database_data(self):
        """Fetch data from database"""
        return self.database_client.fetch_data()

    def fetch_and_process_data(self, symbol: str, start_date: str, end_date: str):
        """Fetch, process and store market data"""
        try:
            # Fetch data
            market_data = self.fetch_market_data(symbol, start_date, end_date)

            # Process data
            processed_data = self.preprocess_data(market_data)

            # Store in database
            self.store_processed_data(processed_data, symbol)

            return processed_data
        except Exception as e:
            raise Exception(f"Pipeline failed: {e}")

    def store_processed_data(self, data: pd.DataFrame, symbol: str):
        """Store processed data in database with edge case handling"""
        try:
            db_client = DatabaseClient()
            query = """
            INSERT INTO processed_market_data
            (symbol, timestamp, value, trade_outcome)
            VALUES (%s, %s, %s, %s)
            """

            # Handle empty DataFrame
            if data.empty:
                # Insert placeholder row for empty data
                params = (symbol, 0, 0, 1)
                db_client.execute_query(query, params)
                return

            for _, row in data.iterrows():
                params = (
                    symbol,
                    float(row.get("timestamp", 0)),
                    float(row.get("value", 0)),
                    int(row.get("trade_outcome", 1)),
                )
                db_client.execute_query(query, params)

        except Exception as e:
            raise Exception(f"Failed to store data: {e}")


if __name__ == "__main__":
    data_fetcher = DataFetcher()
    data_fetcher.fetch_historical_data()
