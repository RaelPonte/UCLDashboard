from common.controllers import DataIngestor, DataProcessor, PipelineReport
from common.config import DATABASE_PATH, DATA_DIRECTORY_PATH, CHUNK_SIZE, REPORT_DIRECTORY_PATH
import os
from datetime import datetime

class DataPipeline:
    def __init__(self, table_filename: str):
        self.table_filename = table_filename
        self.table_name = table_filename.split("/")[-1].split(".")[0]
        self.duckdb_path = DATABASE_PATH
        self.report = PipelineReport(
            csv_file=f"{DATA_DIRECTORY_PATH}/{self.table_filename}",
            table_name=self.table_name,
        )

    def run(self):
        self._process_data()
        self._ingest_data()
        self._save_report()
        self._clean_cache()

    def _process_data(self):
        processor = DataProcessor(csv_file=f"{DATA_DIRECTORY_PATH}/{self.table_filename}", report_obj=self.report)
        processor.process_data()

    def _ingest_data(self):
        with DataIngestor(
            db_path=self.duckdb_path, 
            table_name=self.table_name,
            csv_file=f"{DATA_DIRECTORY_PATH}/{self.table_filename}.processed",
            chunk_size=CHUNK_SIZE,
            report_obj=self.report
        ) as ingestor:
            ingestor.ingest_data()

    def _clean_cache(self):
        processed_path = f"{DATA_DIRECTORY_PATH}/{self.table_filename}.processed"
        progress_path = f"{DATA_DIRECTORY_PATH}/{self.table_filename}.progress"
        
        if os.path.exists(processed_path):
            os.remove(processed_path)
        if os.path.exists(progress_path):
            os.remove(progress_path)
        
    def _save_report(self):
        self.report.finished_at = datetime.now()
        self.report.duration_seconds = (self.report.finished_at - self.report.started_at).total_seconds()
        self.report.rows_per_second = self.report.inserted_rows / self.report.duration_seconds
        self.report.save_to_html(f"{REPORT_DIRECTORY_PATH}/{self.table_filename}_{self.report.started_at.strftime('%Y%m%d_%H%M%S')}.html")

if __name__ == "__main__":
    # Clean database before execution
    if os.path.exists(DATABASE_PATH):
        os.remove(DATABASE_PATH)

    dataset_list = os.listdir(DATA_DIRECTORY_PATH)
    # Put key_stats first
    dataset_list = ["key_stats.csv"] + [f for f in dataset_list if f not in ["key_stats.csv"] and '.csv' in f]
    
    for csv in dataset_list:
        DataPipeline(csv).run()
        # input("Press any key to continue...")
