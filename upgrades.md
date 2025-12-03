  📋 Análise e Soluções Propostas

  1. Impedir CSV duplicado (Deduplicação)

  Solução: Hash-based fingerprint + metadata table

  ✅ Estratégia eficiente (sem ler todo o CSV):
  - Calcular hash MD5/SHA256 do arquivo CSV
  - Armazenar na tabela metadata (já existe no código)
  - Verificar hash antes de processar

```python
import hashlib

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

  Custo: O(1) query no banco + leitura linear do arquivo (inevitável)

  ---
  2. CSV com colunas extras

  Decisão recomendada: Modo flexível com opções

  ✅ Abordagem pragmática:

  Opção A - Intersecção (Recomendado para produção):
  # Usar apenas colunas comuns entre CSV e schema
  common_cols = set(chunk.columns) & set(schema.keys())
  chunk = chunk[list(common_cols)]
  # Log warning sobre colunas ignoradas

  Opção B - Strict Mode (opcional via CLI flag):
  # Rejeitar CSV se tiver colunas extras
  if set(chunk.columns) != set(schema.keys()):
      raise ValueError("Schema mismatch: extra columns found")

  Implementação CLI:
  python main.py ingest dataset.csv --strict-schema  # Rejeita extras
  python main.py ingest dataset.csv  # Aceita e ignora extras (default)

  ---
  3. Erro no meio da importação - Rollback ou Continuar?

  Decisão: Transação por chunk + rollback completo em falha crítica

  ✅ Estratégia híbrida:

  # Nível 1: Transaction por chunk (já existe retry de 3x)
  self.conn.execute("BEGIN TRANSACTION")
  try:
      # ... insert chunk ...
      self.conn.execute("COMMIT")
  except:
      self.conn.execute("ROLLBACK")
      # Retry logic...

  # Nível 2: Rollback completo se falhar após max retries
  # Depende do contexto - CLI deve perguntar ao usuário

  Para o CLI:
  python main.py ingest dataset.csv --on-error rollback  # Remove tudo
  python main.py ingest dataset.csv --on-error continue  # Mantém chunks OK (default)

  Justificativa:
  - Continue (default): Útil para datasets grandes onde 1 chunk ruim não invalida 99 chunks bons
  - Rollback: Útil para dados críticos onde consistência total é mandatória

  ---
  4. Relatório Final

  Estrutura proposta:

  @dataclass
  class IngestionReport:
      """Relatório de ingestão de dados."""

      # Identificação
      csv_file: str
      table_name: str
      started_at: datetime
      finished_at: datetime

      # Estatísticas
      total_chunks: int
      successful_chunks: int
      failed_chunks: int
      total_rows: int
      inserted_rows: int
      skipped_rows: int  # Duplicatas

      # Detalhes de erros
      errors: List[Dict[str, Any]]  # [{chunk_id, error_msg, row_sample}]

      # Validações
      duplicate_import: bool  # CSV já estava no banco?
      schema_warnings: List[str]  # Colunas extras/faltantes

      # Performance
      duration_seconds: float
      rows_per_second: float

      def print_summary(self):
          """Imprime relatório formatado no terminal."""
          print("=" * 60)
          print(f"📊 INGESTION REPORT: {self.table_name}")
          print("=" * 60)
          print(f"✅ Status: {'SUCCESS' if self.successful_chunks == self.total_chunks else 'PARTIAL'}")
          print(f"📁 File: {self.csv_file}")
          print(f"⏱️  Duration: {self.duration_seconds:.2f}s ({self.rows_per_second:.0f} rows/s)")
          print(f"📦 Chunks: {self.successful_chunks}/{self.total_chunks}")
          print(f"📝 Rows: {self.inserted_rows}/{self.total_rows} inserted")

          if self.duplicate_import:
              print("⚠️  WARNING: This CSV was already imported before")

          if self.schema_warnings:
              print(f"⚠️  Schema warnings: {len(self.schema_warnings)}")
              for warning in self.schema_warnings[:3]:
                  print(f"   - {warning}")

          if self.errors:
              print(f"❌ Errors: {len(self.errors)}")
              for error in self.errors[:3]:
                  print(f"   - Chunk {error['chunk_id']}: {error['error_msg']}")

          print("=" * 60)

      def save_to_json(self, output_path: str):
          """Salva relatório completo em JSON."""
          import json
          with open(output_path, 'w') as f:
              json.dump(asdict(self), f, indent=2, default=str)

  ---
  5. CLI Structure

  Framework recomendado: Typer (mais moderno) ou Click

  # main.py
  import typer
  from typing import Optional
  from enum import Enum

  app = typer.Typer()

  class OnErrorStrategy(str, Enum):
      continue_ = "continue"
      rollback = "rollback"

  @app.command()
  def ingest(
      csv_file: str = typer.Argument(..., help="Path to CSV file"),
      table_name: Optional[str] = typer.Option(None, help="Override table name"),
      strict_schema: bool = typer.Option(False, help="Reject CSVs with extra columns"),
      on_error: OnErrorStrategy = typer.Option(
          OnErrorStrategy.continue_, 
          help="Strategy on chunk failure"
      ),
      force: bool = typer.Option(False, help="Force reimport even if already imported"),
      report_json: Optional[str] = typer.Option(None, help="Save report to JSON file"),
      chunk_size: int = typer.Option(10000, help="Rows per chunk"),
  ):
      """Ingest CSV data into DuckDB database."""

      typer.echo(f"🚀 Starting ingestion: {csv_file}")

      pipeline = DataPipeline(
          csv_file=csv_file,
          table_name=table_name,
          strict_schema=strict_schema,
          on_error=on_error.value,
          force_reimport=force,
          chunk_size=chunk_size,
      )

      report = pipeline.run()
      report.print_summary()

      if report_json:
          report.save_to_json(report_json)
          typer.echo(f"📄 Report saved to {report_json}")

      if report.failed_chunks > 0:
          raise typer.Exit(code=1)

  @app.command()
  def list_tables():
      """List all tables in the database."""
      # Implementation...

  @app.command()
  def validate(csv_file: str):
      """Validate CSV before ingestion (dry-run)."""
      # Check schema, encoding, duplicates, etc.

  if __name__ == "__main__":
      app()

  Uso:
  # Básico
  python main.py ingest data/key_stats.csv

  # Avançado
  python main.py ingest data/key_stats.csv \
      --strict-schema \
      --on-error rollback \
      --report-json reports/import.json

  # Forçar reimport
  python main.py ingest data/key_stats.csv --force

  # Validar antes de importar
  python main.py validate data/key_stats.csv

  # Listar tabelas
  python main.py list-tables

  ---
  🎯 Resumo das Decisões

  | Questão        | Decisão                                              | Justificativa                                       |
  |----------------|------------------------------------------------------|-----------------------------------------------------|
  | CSV duplicado  | Hash SHA256 + verificação em metadata                | Eficiente O(1), não reprocessa arquivo              |
  | Colunas extras | Aceitar e ignorar (default) + modo strict (opcional) | Flexibilidade vs. rigor controlado por CLI flag     |
  | Erro em chunk  | Continue (default) + Rollback (opcional)             | Aproveitamento máximo + segurança quando necessário |
  | Relatório      | Dataclass com print formatado + export JSON          | Legibilidade humana + processamento automático      |
  | CLI            | Typer com comandos: ingest, validate, list-tables    | Moderna, type-safe, auto-documentada                |

  ---