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

        self.conn = duckdb.connect(database=self.db_path, read_only=False)

    def ingest_data(self):
        self._make_validations()

        """Ingestão dos dados com tolerância a falhas."""
        try:
            last_chunk = self._get_last_processed_chunk()
            self.logger.info(f"Iniciando ingestão a partir do chunk {last_chunk}")

            for chunk_index, chunk in enumerate(
                pd.read_csv(self.csv_file, chunksize=self.chunk_size)
            ):
                self.logger.info(f"Chunk {chunk_index}/{chunk.shape[0]} processado")
                if chunk_index < last_chunk:
                    continue

                self.logger.info(f"Inserindo chunk {chunk_index}")

                retry_count = 0
                max_retries = 3
                success = False

                while not success and retry_count < max_retries:
                    try:
                        self.conn.execute(
                            f"CREATE TABLE IF NOT EXISTS {self.table_name} AS SELECT * FROM chunk"
                        )
                        self.conn.execute(
                            f"INSERT INTO {self.table_name} SELECT * FROM chunk"
                        )
                        success = True

                    except Exception as e:
                        retry_count += 1
                        self.logger.warning(
                            f"Falha no chunk {chunk_index}. Tentativa {retry_count}. Erro: {e}"
                        )
                        if retry_count >= max_retries:
                            raise RuntimeError(
                                f"Falha ao inserir chunk {chunk_index} após {max_retries} tentativas"
                            )

                self._update_progress(chunk_index)
                self.logger.info(f"Chunk {chunk_index} inserido com sucesso")

            self.logger.info("Ingestão concluída.")
        finally:
            self.logger.info("Finalizando ingestão.")

            print(f"Print da Tabela {self.table_name}")
            self.con.execute(f"SELECT * FROM {self.table_name}").fetch_df()
            self.conn.close()

            os.remove(self.progress_file)

    def _make_validations(self):
        """
        Realiza as validações necessárias antes de iniciar o processamento:
        - Verifica se o arquivo CSV existe e tem a extensão correta
        - Verifica se o banco de dados existe e tem a extensão correta
        """
        self._validate_csv_file()
        self._validate_db_file()

    def _validate_data_enconde(self):
        pass

    def _validate_csv_file(self):
        if not os.path.exists(self.csv_file):
            raise FileNotFoundError(f"Arquivo {self.csv_file} não encontrado")

        if not self.csv_file.endswith(".csv"):
            raise ValueError(f"Arquivo {self.csv_file} não é um arquivo CSV")

        if os.path.getsize(self.csv_file) == 0:
            raise ValueError(f"Arquivo {self.csv_file} é um arquivo vazio")

    def _validate_db_file(self):
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Banco de dados {self.db_path} não encontrado")

        if not self.db_path.endswith(".duckdb"):
            raise ValueError(f"Banco de dados {self.db_path} não é um arquivo DuckDB")

    def _setup_logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("DataIngestor")
        return logger

    def _get_last_processed_chunk(self):
        """Retorna o último índice de chunk processado, se existir."""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                return int(f.read().strip())
        return 0

    def _update_progress(self, chunk_index):
        """Atualiza o índice do último chunk processado."""
        with open(self.progress_file, "w") as f:
            f.write(str(chunk_index))


# Exemplo de uso
ingestor = DataIngestor(
    db_path="test.duckdb",
    table_name="demo_tabela",
    csv_file="./data/giant_tabel.csv",
)
ingestor.ingest_data()
