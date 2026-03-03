from datetime import datetime

from src.resources.sql_queries import SQLQueries
from src.service.db_service import DBService

SQL = SQLQueries()
DB = DBService()

class UtilService:

    def transform_data(self, data):

        transformed = []

        for coin in data:
            transformed.append({
                "coin_id": coin["id"],
                "symbol": coin["symbol"],
                "name": coin["name"],
                "price_usd": coin["current_price"],
                "market_cap": coin["market_cap"],
                "last_updated": datetime.fromisoformat(
                    coin["last_updated"].replace("Z", "")
                )
            })

        return transformed

    def insert_data(self, conn, transformed):
        create_table_query = SQL.create_table_query()
        no_of_rows = DB.exec_query(conn, create_table_query)
        insert_table_query = SQL.insert_data_query()
        no_of = DB.exec_batch(conn, insert_table_query, transformed)





