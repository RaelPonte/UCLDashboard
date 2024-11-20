from duckdb import duckdb
from typing import List
import pandas as pd
import hashlib
import logging


class MetadataController:
    def __init__(self, duckdb_conn: duckdb.Connection):
        self.conn = duckdb_conn
        self.logger = logging.getLogger("MetadataController")

    def register_metadata(
        self,
        csv_path: str,
        row_count: int,
        column_names: List[str],
    ):
        file_hash = self._calculate_hash_from_file(csv_path)

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_control (
                filename STRING,
                file_hash STRING,
                load_date TIMESTAMP,
                row_count INTEGER,
                column_names STRING
            );
        """
        )

        result = self.conn.execute(
            """
            SELECT 1 FROM ingestion_control WHERE file_hash = ?
        """,
            (file_hash,),
        ).fetchone()

        if result:
            self.logger.error("This file has already been imported!")
            raise FileExistsError
        else:
            self.logger.info("Registering metadata...")
            df = pd.read_csv(csv_path)
            row_count = len(df)
            column_names = list(df.columns)

            self.conn.execute(
                """
                INSERT INTO ingestion_control (filename, file_hash, load_date, row_count, column_names)
                VALUES (?, ?, NOW(), ?, ?)
            """,
                (csv_path, file_hash, row_count, str(column_names)),
            )
            self.logger.info("Metadata registered successfully!")

    @staticmethod
    def _calculate_hash_from_file(csv_path: str) -> str:
        """
        Calcula o hash SHA256 de um arquivo.

        :param csv_path: Caminho absoluto do arquivo
        :return: O hash SHA256 do arquivo
        """
        hash_sha256 = hashlib.sha256()
        with open(csv_path, "rb") as f:
            for bloco in iter(lambda: f.read(4096), b""):
                hash_sha256.update(bloco)
        return hash_sha256.hexdigest()
