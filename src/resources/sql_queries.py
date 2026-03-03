class SQLQueries:

    @staticmethod
    def create_table_query():
        query = """
        CREATE TABLE If not exists crypto_prices (
             coin_id VARCHAR2(50) PRIMARY KEY,
             symbol VARCHAR2 (10) NOT NULL,
             name VARCHAR2 (100) NOT NULL,
             price_usd NUMBER(10, 2) NOT NULL,
             market_cap NUMBER(10, 2)  NOT NULL,
             last_updated TIMESTAMP NOT NULL)"""
        return query

    @staticmethod
    def create_quarantined_table_query():
        query = f"""
        CREATE TABLE IF NOT EXISTS quarantined (
            emp_id INTEGER PRIMARY KEY,
             emp_name VARCHAR2 (100) NOT NULL,
             salary NUMBER(10, 2) NOT NULL,
             dept_id INTEGER NOT NULL,
             age INTEGER NOT NULL,
             error_msg VARCHAR(500) NOT NULL)
        """

        return query

    @staticmethod
    def fetch_employees_query():
        query = """select * from employees"""
        return query

    @staticmethod
    def insert_data_query():

        query = f"""
        INSERT INTO crypto_prices (coin_id, symbol, name, price_usd, market_cap, last_updated)
        VALUES (:coin_id, :symbol, :name, :price_usd, :market_cap, :last_updated)
        """
        return query

    @staticmethod
    def insert_quarantined_query():
        query = f"""
           INSERT INTO quarantined (emp_id, emp_name, salary, dept_id, age, error_msg)
           VALUES (:emp_id, :emp_name, :salary, :dept_id, :age, :error_msg)
           """
        return query




