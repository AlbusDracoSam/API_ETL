import oracledb
import os
from pathlib import Path
import json
from src.resources.sql_queries import SQLQueries

sql_queries = SQLQueries()

class DBService:
    def __init__(self):
        self.conn = None

    def fetch_creds(self):
        file_path = Path(__file__).parent / "../resources/db_config.json"
        file_path = file_path.resolve()  # Absolute path
        try:
            with open(file_path) as f:
                db_config = json.load(f)
        except Exception as e:
            print(f"Error while reading file {file_path}")
            raise e

        print("Fetching credentials")
        return db_config

    def connect(self):
        if self.conn is None:
            try:
                db_config = self.fetch_creds()
                self.conn = oracledb.connect(**db_config)
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM dual")
                    print(f"Connected to DB")
                return self.conn
            except oracledb.Error as e:
                print(f"Connection has failed. {e}")
                raise e
        return None

    def exec_query(self, conn, query, params = None):
        if conn is None:
            self.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            if cursor.description is not None:
                return cursor.fetchall()
            conn.commit()
            return cursor.rowcount
        except oracledb.Error as e:
            print(f"Connection has failed. {e}")
            raise e

    def disconnect(self, conn):
        if conn:
            try:
                conn.close()
                print(f"Disconnected from DB")
            except oracledb.Error as e:
                print(f"Disconnection has failed. {e}")
            finally:
                conn = None

    def exec_batch(self, conn, query, data):
        if conn is None:
            conn = self.connect()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, data)
            conn.commit()
            print("Batch inserted into DB")
            return cursor.rowcount
        except oracledb.Error as e:
            print(f"Connection has failed. {e}")
            raise e

    def exec_batch_with_quarantine(self, conn, data, query):
        if conn is None:
            conn = self.connect()
        try:
            inserted_count = 0
            cursor = conn.cursor()
            cursor.executemany(query, data, batcherrors=True)
            errors = cursor.getbatcherrors()
            if errors:
                print(f"Batch inserted into DB with errors: {errors}")
                quarantined_rows = self.quarantine_batch(errors, data)
                quarantine_query = sql_queries.insert_quarantined_query()
                cursor.executemany(quarantine_query, quarantined_rows)
            conn.commit()
            print("Batch inserted into DB")
            conn.commit()
            print("Batch inserted into DB")
            return cursor.rowcount
        except oracledb.Error as e:
            print(f"Connection has failed. {e}")
            raise e

    def quarantine_batch(self, errors, df):
        try:
            quarantine_rows = []
            for err in errors:
                row = df.iloc[err.offset]
                quarantine_rows.append({
                    "emp_id": int(row.emp_id),
                    "emp_name": str(row.emp_name),
                    "salary": float(row.salary),
                    "dept_id": int(row.dept_id),
                    "age": int(row.age),
                    "error_msg": str(err.message)
                })
            return quarantine_rows
        except Exception as e:
            print("Error while processing quarantine")
            raise

