import duckdb
import pandas as pd
import os
import logging


class DataIngestor:
    def __init__(
        self, db_path: str, table_name: str, csv_file: str, chunk_size: int = 10000
    ):
        self.db_path = db_path
        self.table_name = table_name
        self.csv_file = csv_file
        self.chunk_size = chunk_size
        self.progress_file = f"{csv_file}.progress"
        self.logger = self._setup_logger()
        self._make_startup_validations()

        self.conn = duckdb.connect(database=self.db_path)

    def ingest_data(self):
        """
        Realiza a ingestão dos dados do arquivo CSV para o banco de dados.

        Realiza a validação dos arquivos de entrada e sa da, e se a ingestão
        já foi iniciada anteriormente, pula para o último chunk processado.

        Tenta realizar a inserção de cada chunk em um loop, e caso haja falha,
        realiza até 3 tentativas antes de lançar um erro.

        Remove o arquivo de progresso ao final da ingestão.
        """
        try:
            self._ingest_chunks_in_db()
            self.logger.info("Print da Tabela %s", self.table_name)
            print(self.conn.execute(f"SELECT * FROM {self.table_name}").fetchdf())
        finally:
            self.conn.close()
            os.remove(self.progress_file)

    def _ingest_chunks_in_db(self):
        last_chunk = self._get_last_processed_chunk()
        self.logger.info("Iniciando ingestão a partir do chunk %d", last_chunk)

        if not self._table_exists():
            schema = self._get_csv_schema()
        else:
            schema = self._get_existing_table_schema()
        self.logger.info("Schema obtido: %s", schema)

        chunks_total = int(pd.read_csv(self.csv_file).shape[0] / self.chunk_size)

        for chunk_index, chunk in enumerate(
            pd.read_csv(self.csv_file, chunksize=self.chunk_size)
        ):
            self.logger.info("Chunk %d/%d processado", chunk_index + 1, chunks_total)
            if chunk_index < last_chunk:
                continue

            self._validate_chunk_against_schema(chunk, schema)
            self._validate_chunk_encoding(chunk, "utf-8")

            self.logger.info("Inserindo chunk %d", chunk_index + 1)

            retry_count = 0
            max_retries = 3
            success = False

            while not success and retry_count < max_retries:
                try:
                    self.conn.register("chunk", chunk)

                    self.conn.execute(
                        f"CREATE TABLE IF NOT EXISTS {self.table_name} AS SELECT * FROM chunk LIMIT 0"
                    )
                    self.conn.execute(
                        f"INSERT INTO {self.table_name} SELECT * FROM chunk"
                    )
                    success = True

                except Exception as e:
                    retry_count += 1
                    self.logger.warning(
                        "Falha no chunk %d. Tentativa %d. Erro: %s",
                        chunk_index + 1,
                        retry_count,
                        e,
                    )
                    if retry_count >= max_retries:
                        raise RuntimeError(
                            f"Falha ao inserir chunk {chunk_index + 1} após {max_retries} tentativas"
                        ) from e

            self._update_progress(chunk_index)

        self.logger.info("Ingestão concluída.")

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
        self.logger.info(f"Validando chunk {chunk_index} com base no schema")
        for column, expected_type in schema.items():
            if column in chunk.columns:
                actual_type = str(chunk[column].dtype.name)
                expected_type = str(expected_type)
                if expected_type != actual_type:
                    raise ValueError(
                        f"O tipo de dado da coluna '{column}' no chunk não corresponde ao esperado. "
                        f"Esperado: {expected_type}, Obtido: {actual_type}"
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
        for column in chunk.select_dtypes(include=[object]):
            for value in chunk[column]:
                if isinstance(value, str):
                    try:
                        value = value.encode(encoding)
                    except UnicodeEncodeError:
                        raise ValueError(
                            f"Valor '{value}' na coluna '{column}' não pode ser "
                            f"codificado como {encoding}"
                        )
                    except LookupError:
                        raise ValueError(
                            f"Encoding '{encoding}' não é um encoding suportado pelo Python"
                        )

    def _validate_csv_file(self):
        if not os.path.exists(self.csv_file):
            raise FileNotFoundError(f"Arquivo {self.csv_file} não encontrado.")

        if not self.csv_file.endswith(".csv"):
            raise ValueError(f"Arquivo {self.csv_file} não é um arquivo CSV.")

        if os.path.getsize(self.csv_file) == 0:
            raise ValueError(f"Arquivo {self.csv_file} é um arquivo vazio.")

    def _validate_db_file(self):
        if self.db_path == ":memory:":
            return
        if not self.db_path.endswith(".duckdb"):
            raise ValueError(f"Banco de dados {self.db_path} não é um arquivo DuckDB")

    def _setup_logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("DataIngestor")
        return logger

    def _get_last_processed_chunk(self):
        """Retorna o último índice de chunk processado, se existir."""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        return 0

    def _update_progress(self, chunk_index):
        """Atualiza o índice do último chunk processado."""
        with open(self.progress_file, "w", encoding="utf-8") as f:
            f.write(str(chunk_index))


# Exemplo de uso
ingestor = DataIngestor(
    db_path="test.duckdb",  # ":memory:",
    table_name="demo",
    csv_file="/home/israelponte/my_repos/dashboard/data/test.csv",
)
ingestor.ingest_data()
