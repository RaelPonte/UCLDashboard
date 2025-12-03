from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any
from dataclasses import asdict
import logging
import json
from dataclasses import field
import os

logger = logging.getLogger(__name__)

@dataclass
class PipelineReport:
    """Relatório de ingestão de dados."""

    # Identificação
    csv_file: str
    table_name: str
    started_at: datetime = field(default=datetime.now())
    finished_at: datetime = field(default=None)

    # Estatísticas
    total_chunks: int = field(default=0)
    successful_chunks: int = field(default=0)
    failed_chunks: int = field(default=0)
    total_rows: int = field(default=0)
    inserted_rows: int = field(default=0)
    skipped_rows: int = field(default=0)  # Duplicatas

    # Detalhes de erros
    errors: List[Dict[str, Any]] = field(default_factory=list)  # [{module, chunk_id, error_msg, row_sample}]

    # Validações
    duplicate_import: bool = field(default=False)  # CSV já estava no banco?
    schema_warnings: List[str] = field(default_factory=list)  # Colunas extras/faltantes

    # Performance
    duration_seconds: float = field(default=0.0)
    rows_per_second: float = field(default=0.0)

    def print_summary(self):
        """Imprime relatório formatado no terminal."""
        logger.info("=" * 60)
        logger.info(f"📊 INGESTION REPORT: {self.table_name}")
        logger.info("=" * 60)
        logger.info(f"✅ Status: {'SUCCESS' if self.successful_chunks == self.total_chunks else 'PARTIAL'}")
        logger.info(f"📁 File: {self.csv_file}")
        logger.info(f"⏱️  Duration: {self.duration_seconds:.2f}s ({self.rows_per_second:.0f} rows/s)")
        logger.info(f"📦 Chunks: {self.successful_chunks}/{self.total_chunks}")
        logger.info(f"📝 Rows: {self.inserted_rows}/{self.total_rows} inserted")

        if self.duplicate_import:
            logger.warning("⚠️  WARNING: This CSV was already imported before")

        if self.schema_warnings:
            logger.warning(f"⚠️  Schema warnings: {len(self.schema_warnings)}")
            for warning in self.schema_warnings[:3]:
                logger.warning(f"   - {warning}")

        if self.errors:
            logger.warning(f"❌ Errors: {len(self.errors)}")
            for error in self.errors[:3]:
                logger.warning(f"   - {error['module']} Chunk {error['chunk_id']}: {error['error_msg']}")

        logger.info("=" * 60)

    def save_to_json(self, output_path: str):
        """Salva relatório completo em JSON."""
        logger.info(f"Saving report to {output_path}")
        with open(output_path, 'w') as f:
            json.dump(asdict(self), f, indent=2, default=str)

        logger.info("Report saved successfully")

    def save_to_html(self, output_path: str):
        """Salva relatório completo em HTML usando template."""
        logger.info(f"Saving HTML report to {output_path}")

        # Carrega template
        template_path = os.path.join(
            os.path.dirname(__file__),
            '../templates/report_template.html'
        )

        with open(template_path, 'r', encoding='utf-8') as f:
            template = f.read()

        # Prepara variáveis
        is_success = self.successful_chunks == self.total_chunks
        status_class = "success" if is_success else "partial"
        status_text = "SUCCESS" if is_success else "PARTIAL"
        status_icon = "✅" if is_success else "⚠️"

        chunk_progress = (self.successful_chunks / self.total_chunks * 100) if self.total_chunks > 0 else 0

        # Gera seções de warnings e errors
        warnings_section = self._generate_warnings_html()
        errors_section = self._generate_errors_html()

        # Substitui variáveis no template
        html_content = template.replace('{{table_name}}', self.table_name)
        html_content = html_content.replace('{{status_class}}', status_class)
        html_content = html_content.replace('{{status_text}}', status_text)
        html_content = html_content.replace('{{status_icon}}', status_icon)
        html_content = html_content.replace('{{csv_file}}', self.csv_file)
        html_content = html_content.replace('{{duration_seconds}}', f'{self.duration_seconds:.2f}')
        html_content = html_content.replace('{{rows_per_second}}', f'{self.rows_per_second:.0f}')
        html_content = html_content.replace('{{successful_chunks}}', str(self.successful_chunks))
        html_content = html_content.replace('{{total_chunks}}', str(self.total_chunks))
        html_content = html_content.replace('{{chunk_progress}}', f'{chunk_progress:.1f}')
        html_content = html_content.replace('{{inserted_rows}}', f'{self.inserted_rows:,}')
        html_content = html_content.replace('{{total_rows}}', f'{self.total_rows:,}')
        html_content = html_content.replace('{{warnings_section}}', warnings_section)
        html_content = html_content.replace('{{errors_section}}', errors_section)
        html_content = html_content.replace('{{started_at}}', self.started_at.strftime('%Y-%m-%d %H:%M:%S'))
        html_content = html_content.replace('{{finished_at}}',
            self.finished_at.strftime('%Y-%m-%d %H:%M:%S') if self.finished_at else 'N/A')
        html_content = html_content.replace('{{generated_at}}', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # Salva arquivo
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info("HTML report saved successfully")

    def _generate_warnings_html(self) -> str:
        """Gera HTML para warnings."""
        if not self.duplicate_import and not self.schema_warnings:
            return ""

        warnings_html = []

        if self.duplicate_import:
            warnings_html.append("""
            <div class="alert warning">
                <div class="icon">⚠️</div>
                <div class="content">
                    <div class="title">Duplicate Import Warning</div>
                    <p>This CSV file was already imported before.</p>
                </div>
            </div>
            """)

        if self.schema_warnings:
            warnings_list = "\n".join([f"<li>{warning}</li>" for warning in self.schema_warnings[:5]])
            warnings_html.append(f"""
            <div class="alert warning">
                <div class="icon">⚠️</div>
                <div class="content">
                    <div class="title">Schema Warnings ({len(self.schema_warnings)})</div>
                    <ul>
                        {warnings_list}
                    </ul>
                </div>
            </div>
            """)

        return "\n".join(warnings_html)

    def _generate_errors_html(self) -> str:
        """Gera HTML para erros."""
        if not self.errors:
            return ""

        errors_list = "\n".join([
            f"<li><strong>{error.get('module', 'Unknown')}</strong> - Chunk {error.get('chunk_id', 'N/A')}: {error.get('error_msg', 'No message')}</li>"
            for error in self.errors[:5]
        ])

        return f"""
        <div class="alert error">
            <div class="icon">❌</div>
            <div class="content">
                <div class="title">Errors ({len(self.errors)})</div>
                <ul>
                    {errors_list}
                </ul>
            </div>
        </div>
        """