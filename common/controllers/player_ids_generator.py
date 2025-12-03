from duckdb import DuckDBPyConnection
from pandas import DataFrame
import logging
from common.config import PLAYERS_TABLE_NAME

class PlayerIdsGenerator:
    def __init__(self, db_conn: DuckDBPyConnection):
        self.conn = db_conn
        self.table_name = PLAYERS_TABLE_NAME
        self.logger = logging.getLogger(__name__)

    def create_ids(self, chunk: DataFrame):
        self.logger.info("Creating player IDs...")
        try:
            self.conn.register("chunk", chunk)
            
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_name} (id INTEGER PRIMARY KEY, player_name TEXT, club TEXT, UNIQUE(player_name, club))"
            )

            self.conn.execute(
                "CREATE SEQUENCE IF NOT EXISTS player_id_seq START WITH 1 INCREMENT BY 1"
            )

            self.conn.execute(
                f"""
                INSERT OR IGNORE INTO {self.table_name}
                SELECT nextval('player_id_seq') AS id, player_name, club
                FROM chunk
                """
            )
        
            self.logger.info("Player IDs created successfully.")

        except Exception as e:
            self.logger.error(f"Failed to create player IDs: {e}")