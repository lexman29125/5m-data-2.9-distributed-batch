# Assignment

## Brief

## Instructions

### Question 1

Question: Write a Python function that takes a PySpark DataFrame `df` and a column name `col` as input, and returns a new DataFrame with a new column `new_col` that contains the square root of the values in `col`.

```python
def add_sqrt_column(df, col):
    # your code here
    return new_df
```

Answer:

```python
# answer code
from pyspark.sql.functions import sqrt

def add_sqrt_column(df, col):
    new_df = df.withColumn("new_col", sqrt(col))
    return new_df
```

### Question 2

Question: How would you read a JSON file into a PySpark DataFrame?

```python
json_file = "path/to/data.json"
```

Answer:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.json(json_file)
```

### Question 3

Question: Write a PySpark code snippet to filter a DataFrame `df` to only include rows where the value in the `column1` column is greater than 10 and the value in the `column2` column contains the substring "foo".

```python
# question code
from pyspark.sql.functions import col

filtered_df = df.filter(...)
```

Answer:

```python
from pyspark.sql.functions import col, instr

filtered_df = df.filter((col("column1") > 10) & (instr(col("column2"), "foo") > 0))
```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
