# Iceberg connector
## Example workflow
```python
import pandas as pd
from viadot.sources import Iceberg
import os

livy_url = os.environ["LIVY_URL"]
livy_credentials = (os.environ["LIVY_USERNAME"], os.environ["LIVY_PASSWORD"])
iceberg = Iceberg(url=livy_url, credentials=livy_credentials)
df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

created = iceberg.from_df(df=df, table="test_table")
assert created is True

test_df = iceberg.to_df("test_table")
assert not test_df.equals(df)
```

## How could it work?
### `from_df()`
#### `Iceberg`
```python
def from_df(self, df, table):
    sdf = SparkDataFrame.from_pandas(session=self.session, df=df)
    sdf.to_iceberg(table=table)
```

#### `SparkDataFrame`
We want this as a class because in the future,
it can have other useful stuff, such as `sdf.head()` etc.
Also, it can be reused by both IcebergSource and IcebergDestination.

```python
def __init__(self, session, df):
    self.session = session

    # Create a Spark DataFrame and expose as the
    # 'sdf' variable in the current session.
    # This prototype only supports creating spark df from a pandas df
    df_json = df.to_json()
    self.session.run(
        f"""
import pandas as pd
df = pd.from_json({df_json})
sfd = spark.DataFrame.from_pandas(df)
""")

@classmethod
def from_pandas(cls, session, df):
    return cls(session=session, df=df)

def to_iceberg(self, table):
    self.session.run(f"sdf.writeTo({table})")
```

### `to_df()`
#### `Iceberg`
```python
def to_df(self, table):
    return self.session.run(f"select * from {table}", sql=True)
```

#### `LivySession`
```python
def run(self, code, sql=False):
    if sql:
        code = f"""
        spark = SparkSession.builder.getOrCreate()
        spark.sql({code})
        """
        return self.run(code=code)
    
    data = {"code": code}
    response = self.post(self._statements_url(self.id), data=data)
    df = self.parse_response_to_df()
    return df

def parse_response_to_df(response: dict):
    result = response["the_result_field"]
    return pd.DataFrame.from_json(result)
```