import os
import pandas as pd
import json


def get_columns_info():
    # List all CSV files in the 'data' directory
    csvs = os.listdir("data")
    columns_info = {}

    for csv in csvs:
        if csv.endswith(".csv"):
            # Read the CSV into a DataFrame
            df = pd.read_csv(os.path.join("data", csv))

            # Generate the column info with data type mapping
            columns_info[csv] = {}
            for col, dtype in df.dtypes.items():
                if dtype == "int64":
                    columns_info[csv][col] = "integer"
                elif dtype == "object":
                    max_length = max(len(str(x)) for x in df[col])
                    columns_info[csv][col] = f"varchar({max_length})"
                elif dtype == "float64":
                    columns_info[csv][col] = "float"
                elif dtype == "bool":
                    columns_info[csv][col] = "boolean"

    # Write the column info to a JSON file
    with open("columns_info.json", "w", encoding="utf-8") as f:
        json.dump(columns_info, f, indent=4)


if __name__ == "__main__":
    get_columns_info()
