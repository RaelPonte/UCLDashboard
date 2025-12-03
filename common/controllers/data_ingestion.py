import duckdb
import pandas as pd
import numpy as np
import os
import logging
import hashlib
from .metadata_controller import MetadataController
from common.exception import AlreadyImportedError
from .player_ids_generator import PlayerIdsGenerator
from .pipeline_report import PipelineReport

class DataIngestor:
    def __init__(
        self, db_path: str, table_name: str, csv_file: str, chunk_size: int, report_obj: PipelineReport
    ):
        self.db_path = db_path
        self.table_name = table_name
        self.csv_file = csv_file
        self.chunk_size = chunk_size
        self.progress_file = f"{csv_file}.progress"
        self.chunk_index = 0
        self.conn = None
        self.metadata_controller = None
        self.report_obj = report_obj
        self.logger = self._setup_logger()
        self._make_startup_validations()

    def ingest_data(self):
        """
        Realiza a ingestão dos dados do arquivo CSV para o banco de dados.

        Realiza a validação dos arquivos de entrada e saída, e se a ingestão
        já foi iniciada anteriormente, pula para o último chunk processado.

        Tenta realizar a inserção de cada chunk em um loop, e caso haja falha,
        realiza até 3 tentativas antes de lançar um erro.

        Remove o arquivo de progresso ao final da ingestão.
        """
        self.logger.info("Starting ingestion for table '%s'", self.table_name)
        self._ingest_chunks_in_db()
        self.logger.info("Table '%s' ingestion completed.", self.table_name)
    
        print(self.conn.execute(f"SELECT * FROM {self.table_name}").fetchdf())

    def __enter__(self):
        self.conn = duckdb.connect(database=self.db_path)
        self.metadata_controller = MetadataController(self.conn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)

    def _ingest_chunks_in_db(self):
        last_chunk = self._get_last_processed_chunk()
        self.logger.info("Starting ingestion from chunk %d", last_chunk)

        if self._check_if_already_imported(self.csv_file):
            self.report_obj.duplicate_import = True
            raise AlreadyImportedError("CSV already imported.")
        
        if not self._table_exists():
            schema = self._get_csv_schema()
        else:
            schema = self._get_existing_table_schema()
        self.logger.info("Schema obtained: %s", schema)

        rows_lenght = sum(1 for _ in open(self.csv_file)) - 1

        chunks_total = int(rows_lenght / self.chunk_size) if rows_lenght > self.chunk_size else 1

        dataframe = pd.read_csv(self.csv_file)

        if 'serial' in dataframe.columns:
            dataframe = dataframe.drop(columns=['serial']) 

        # Divide dataframe in chunks
        dataframe = np.array_split(dataframe, chunks_total)
        self.report_obj.total_chunks = chunks_total
        self.report_obj.total_rows = rows_lenght

        for chunk_index, chunk in enumerate(dataframe):
            self.chunk_index = chunk_index
            self.logger.info("Chunk %d/%d processado", self.chunk_index + 1, chunks_total)
            if self.chunk_index < last_chunk:
                self.report_obj.successful_chunks += 1
                continue

            self._validate_chunk_against_schema(chunk, schema)
            self._validate_chunk_encoding(chunk, "utf-8")

            self.logger.info("Inserting chunk %d", self.chunk_index + 1)

            retry_count = 0
            max_retries = 3
            success = False

            players_ids_generator = PlayerIdsGenerator(self.conn)

            while not success and retry_count < max_retries:
                try:
                    # For player in chunk dataframe, we need to register it on the 'players' table
                    # If not already registered
                    players_ids_generator.create_ids(chunk)

                    # For each player, we'll insert correspondent PlayerID from 'players' table
                    # Can only update base table
                    # So we need to create a temporary table
                    # With 'player_id' column
                    self.conn.execute("DROP TABLE IF EXISTS temp_chunk")
                    self.conn.execute(
                        """
                        CREATE TABLE temp_chunk AS SELECT * FROM chunk;
                        ALTER TABLE temp_chunk ADD COLUMN player_id INTEGER
                        """
                    )

                    self.conn.execute(
                        """
                        UPDATE temp_chunk
                        SET player_id = (
                            SELECT id
                            FROM players    
                            WHERE player_name = temp_chunk.player_name
                            AND club = temp_chunk.club
                        )
                        """
                    )


                    ## Create table if not exists and define 'player_id' as primary key
                    # Check if table exists before trying to add primary key
                    table_exists = self._table_exists()

                    if not table_exists:
                        # Create the table structure from temp_chunk
                        self.conn.execute(
                            f"""
                            CREATE TABLE {self.table_name} AS 
                            SELECT * FROM temp_chunk LIMIT 0;
                            """
                        )
                        # Add primary key only when creating the table for the first time
                        self.conn.execute(
                            f"ALTER TABLE {self.table_name} ADD PRIMARY KEY (player_id);"
                        )

                    self.metadata_controller.register_chunk_metadata(
                        csv_path=self.csv_file,
                        chunk_dataframe=chunk,
                    )

                    self.conn.execute(
                        f"INSERT INTO {self.table_name} SELECT * FROM temp_chunk"
                    )

                    chunk_rows_inserted = self.conn.execute(
                        f"SELECT COUNT(*) FROM temp_chunk"
                    ).fetchone()[0]

                    self.report_obj.inserted_rows += chunk_rows_inserted

                    self.conn.execute("DROP TABLE temp_chunk")
                    self.report_obj.successful_chunks += 1
                    success = True

                except AlreadyImportedError:
                    self.report_obj.duplicate_imported_rows = True
                    success = True

                except Exception as e:
                    retry_count += 1
                    self.logger.warning(
                        "Error on chunk %d. Try Number %d. Error: %s",
                        chunk_index + 1,
                        retry_count,
                        e,
                    )
                    if retry_count >= max_retries:
                        self.report_obj.errors.append(
                            {
                                "module": "data_ingestion",
                                "chunk_id": chunk_index,
                                "error_msg": f"Failed to insert chunk {chunk_index + 1} after {max_retries} attempts",
                                "row_sample": None,
                            }
                        )
                        self.report_obj.failed_chunks += 1
                        raise RuntimeError(
                            f"Failed to insert chunk {chunk_index + 1} after {max_retries} attempts"
                        ) from e

            self._update_progress(chunk_index)

        self.logger.info("Ingestion completed.")

    def _get_csv_schema(self):
        return pd.read_csv(self.csv_file).dtypes.to_dict()

    def _get_existing_table_schema(self):
        """
        Recupera o schema da tabela existente no DuckDB.

        Returns:
            dict: Um dicionário com os nomes das colunas e seus tipos de dados.
        """
        # Mapa de tipos de dados para os tipos de dados do DuckDB
        # Foi criado para garantir que o tipo de dado do DuckDB seja o mesmo do Pandas
        type_mapping = {
            "INTEGER": "int64",
            "BIGINT": "int64",
            "DOUBLE": "float64",
            "VARCHAR": "object",
            "BOOLEAN": "bool",
            "DATE": "datetime64[ns]",
            "TIMESTAMP": "datetime64[ns]",
        }

        schema_info = self.conn.execute(
            f"PRAGMA table_info({self.table_name})"
        ).fetchall()
        schema = {
            column[1]: type_mapping.get(column[2], column[2]) for column in schema_info
        }
        return schema

    def _table_exists(self) -> bool:
        """
        Verifica se a tabela especificada já existe no banco de dados.

        Returns:
            bool: True se a tabela existir, False caso contrário.
        """
        result = self.conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.table_name}'"
        ).fetchone()
        return result[0] > 0

    def _validate_chunk_against_schema(self, chunk, schema):
        """
        Valida os tipos de dados no chunk com base no schema da tabela.

        Args:
            chunk (pd.DataFrame): O chunk de dados a ser validado.
            schema (dict): O schema da tabela, como {nome_da_coluna: tipo_de_dado}.

        Raises:
            ValueError: Se algum valor no chunk não corresponder ao tipo de dado esperado.
        """
        self.logger.info(f"Validando chunk {self.chunk_index} com base no schema")
        for column, expected_type in schema.items():
            if column in chunk.columns:
                actual_type = str(chunk[column].dtype.name)
                expected_type = str(expected_type)
                if expected_type != actual_type:
                    self.report_obj.errors.append(
                        {
                            "module": "data_ingestion",
                            "chunk_id": self.chunk_index,
                            "error_msg": f"The data type of column '{column}' in the chunk does not match the expected type. "
                            f"Expected: {expected_type}, Obtained: {actual_type}",
                            "row_sample": chunk[column].head(1).to_dict(),
                        }
                    )
                    raise ValueError(
                        f"The data type of column '{column}' in the chunk does not match the expected type. "
                        f"Expected: {expected_type}, Obtained: {actual_type}"
                    )

    def _make_startup_validations(self):
        """
        Realiza as validações necessárias antes de iniciar o processamento:
        - Verifica se o arquivo CSV existe e tem a extensão correta
        - Verifica se o banco de dados existe e tem a extensão correta
        """
        self._validate_csv_file()
        self._validate_db_file()

    def _validate_chunk_encoding(self, chunk, encoding="utf-8"):
        """
        Valida se todas as strings no chunk podem ser codificadas em UTF-8.

        Args:
            chunk (pd.DataFrame): O chunk de dados a ser validado.
            encoding (str): O encoding a ser validado (padrão é 'utf-8').

        Raises:
            ValueError: Se alguma string não puder ser codificada em UTF-8.
        """


    def _validate_csv_file(self):
        error_obj = {
            "module": "data_ingestion",
            "chunk_id": self.chunk_index,
            "error_msg": None,
            "row_sample": None,
        }

        if not os.path.exists(self.csv_file):
            error_obj["error_msg"] = f"File {self.csv_file} not found."
            self.report_obj.errors.append(error_obj)
            raise FileNotFoundError(f"File {self.csv_file} not found.")

        if not self.csv_file.endswith(".csv.processed"):
            error_obj["error_msg"] = f"File {self.csv_file} is not a processed CSV file."
            self.report_obj.errors.append(error_obj)
            raise ValueError(f"File {self.csv_file} is not a processed CSV file.")

        if os.path.getsize(self.csv_file) == 0:
            error_obj["error_msg"] = f"File {self.csv_file} is an empty file."
            self.report_obj.errors.append(error_obj)
            raise ValueError(f"File {self.csv_file} is an empty file.")

    def _validate_db_file(self):
        if self.db_path == ":memory:":
            return
        if not self.db_path.endswith(".duckdb"):
            error_obj = {
                "module": "data_ingestion",
                "chunk_id": self.chunk_index,
                "error_msg": None,
                "row_sample": None,
            }
            error_obj["error_msg"] = f"Database file {self.db_path} is not a DuckDB file."
            self.report_obj.errors.append(error_obj)
            raise ValueError(f"Database file {self.db_path} is not a DuckDB file.")

    def _setup_logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("DataIngestor")
        return logger

    def _get_last_processed_chunk(self):
        """Returns the last processed chunk index, if it exists."""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        else:
            with open(self.progress_file, "w", encoding="utf-8") as f:
                f.write("0")
            return 0

    def _update_progress(self, chunk_index):
        """Updates the last processed chunk index."""
        with open(self.progress_file, "w", encoding="utf-8") as f:
            f.write(str(chunk_index))

    def _calculate_file_hash(self, filepath: str) -> str:
        """Calcula hash SHA256 do arquivo sem carregar tudo na memória."""
        hasher = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _check_if_already_imported(self, csv_path: str) -> bool:
        """Verifica se CSV já foi importado."""
        file_hash = self._calculate_file_hash(csv_path)
        result = self.conn.execute(
            "SELECT COUNT(*) FROM metadata WHERE file_hash = ?",
            [file_hash]
        ).fetchone()
        return result[0] > 0

