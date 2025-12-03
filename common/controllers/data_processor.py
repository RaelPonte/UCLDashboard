import pandas as pd
import logging
from .pipeline_report import PipelineReport
import os

class DataProcessor:
    def __init__(self, csv_file: str, report_obj: PipelineReport):
        """
        Only CSV files are supported
        """
        self.csv_file = csv_file
        self.logger = logging.getLogger("DataProcessor")
        self.report_obj = report_obj

    def process_data(self):
        self._validate_fields()
        self.logger.info(f"Processing data of {self.csv_file}...")

        df = pd.read_csv(self.csv_file)
        self._validate_encoding(df)

        # Remove duplicated rows
        dropped_rows = df.duplicated().sum()
        self.report_obj.skipped_rows = int(dropped_rows)
        df.drop_duplicates(inplace=True)
        self.logger.info("Duplicated rows removed successfully!")

        # Saving processed file
        df.to_csv(self.csv_file + ".processed", index=False)
        self.logger.info("Processed file saved successfully!")

        self.logger.info("Data processed successfully!")

    def _validate_fields(self):
        self.logger.info("Validating fields...")
        if self.csv_file is None:
            raise ValueError("CSV file path is required")

        if not os.path.exists(self.csv_file):
            raise FileNotFoundError(f"CSV file not found: {self.csv_file}")

        if not self.csv_file.endswith(".csv"):
            raise ValueError("CSV file path must end with .csv")

        self.logger.info("Fields validated successfully!")
    
    def _validate_encoding(self, df: pd.DataFrame):
        self.logger.info("Validating encoding...")
        for column in df.select_dtypes(include=[object]):
            for value in df[column].values:
                if isinstance(value, str):
                    try:
                        value = value.encode("utf-8")
                    except UnicodeEncodeError:
                        self.report_obj.errors.append(
                            {
                                "module": "DataProcessor",
                                "chunk_id": None,
                                "error_msg": f"Value '{value}' in column '{column}' cannot be encoded as utf-8",
                                "row_sample": value,
                            }
                        )
                        raise ValueError(
                            f"Value '{value}' in column '{column}' cannot be "
                            f"encoded as utf-8"
                        )

        self.logger.info("Encoding validated successfully!")

    def _validate_business_rules(self, df: pd.DataFrame):
        self.logger.info("Validating business rules...")

        for column in df.select_dtypes(include=[int, float]).columns:
            if (df[column] < 0).any():
                self.report_obj.errors.append(
                    {
                        "module": "DataProcessor",
                        "chunk_id": None,
                        "error_msg": f"{column} cannot be negative",
                        "row_sample": df[df[column] < 0].head(1).to_dict(orient="records")[0],
                    }
                )
                raise ValueError(f"{column} cannot be negative")

        self.logger.info("Business rules validated successfully!")