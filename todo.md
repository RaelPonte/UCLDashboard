Base: https://www.kaggle.com/datasets/azminetoushikwasi/ucl-202122-uefa-champions-league?select=defending.csv

## Challenges

- Discover the weak points of any team.
- Suggest players need to be sold, based on performance analysis.
- Nominate Player of the season

## TODO:

1. Layer for Query/Access to the data

- Create a class named 'DataReader' or 'QueryController' to execute queries on the DuckDB database.
- Implement specific methods for aggregations (e.g., top scorers, statistics by team).

2. Centralize Configuration

- For don't repeat the same code in multiple places, create a 'config.py' file to store constants and configurations variables.

3. Validate Bussines Data

- e.g.: Validate if the goals are >= 0
- e.g.: ...

