import duckdb
import numpy


class DuckDBController:
    def __init__(self):

        self.con = None

    def connect(self):
        self.con = duckdb.connect("ucl_db.duckdb")
        
    def close(self):
        self.con.close()

    def create_table_from_csv(self, table_name: str, csv_path: str):
        print(f"Creating table {table_name} from CSV...")
        self.con.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        )
        print(f"Table {table_name} created successfully!")

    def select_all_from_table(self, table_name: str):
        print(f"Selecting all from table {table_name}...")
        return self.con.execute(f"SELECT * FROM {table_name}").fetch_df()
    
