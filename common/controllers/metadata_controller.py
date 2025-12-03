from common.exception import AlreadyImportedError
import pandas as pd
import hashlib
import logging


class MetadataController:
    def __init__(self, duckdb_conn):
        self.conn = duckdb_conn
        self.logger = logging.getLogger("MetadataController")
        self.create_metadata_table()

    def register_chunk_metadata(
        self,
        csv_path: str,
        chunk_dataframe: pd.DataFrame,
    ):
        file_hash = self._calculate_hash_from_file(csv_path)

        result = self.conn.execute(
            """
            SELECT 1 FROM metadata WHERE file_hash = ?
        """,
            (file_hash,),
        ).fetchone()

        if result:
            raise AlreadyImportedError
        else:
            self.logger.info("Registering metadata...")

            self.conn.execute(
                """
                INSERT INTO metadata (filename, file_hash, load_date, row_count, column_names)
                VALUES (?, ?, NOW(), ?, ?)
            """,
                (csv_path, file_hash, len(chunk_dataframe), str(chunk_dataframe.columns)),
            )
            self.logger.info("Metadata registered successfully!")

    def create_metadata_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                filename STRING,
                file_hash STRING,
                load_date TIMESTAMP,
                row_count INTEGER,
                column_names STRING
            );
        """
        )

    @staticmethod
    def _calculate_hash_from_file(csv_path: str) -> str:
        """
        Calcula o hash SHA256 de um arquivo.

        :param csv_path: Caminho absoluto do arquivo
        :return: O hash SHA256 do arquivo
        """
        hash_sha256 = hashlib.sha256()
        with open(csv_path, "rb") as f:
            for block in iter(lambda: f.read(4096), b""):
                hash_sha256.update(block)
        return hash_sha256.hexdigest()
