# AWS S3 Operations

This repository contains a package for various i/o functions on s3 buckets.

### To install

`pip install aws_s3_ops`

### To uninstall

`pip uninstall aws_s3_ops`

### Usage

The list of available functions are:

- [`save_pickle`](####save_pickle)
- [`load_pickle`](####load_pickle)
- [`save_csv`](####save_csv)
- [`save_json`](####save_json)
- [`download_file`](####download_file)
- [`upload_file`](####upload_file)
- [`key_exists`](####key_exists)
- [`delete_data`](####delete_data)
- [`get_prefix_object`](####get_prefix_object)
- [`get_file_buffer`](####get_file_buffer)


#### save_pickle
```python
from aws_s3_ops.aws_s3_ops import S3Operations

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/pickle.pkl"
obj = RandomClassObject()

obj_s3.save_pickle(bucket=bucket, key=key, obj=obj)  # Returns boolean
```
#### load_pickle
```python
from aws_s3_ops.aws_s3_ops import S3Operations

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/pickle.pkl"

obj = obj_s3.load_pickle(bucket=bucket, key=key)  # Loads unpickled object from s3
```

#### save_csv

```python
from aws_s3_ops.aws_s3_ops import S3Operations
import pandas as pd

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/file.csv"
df = pd.DataFrame([['a','b'],['c', 'd']], columns=['col1', 'col2'])

obj_s3.save_csv(bucket=bucket, key=key, df=df, index=False)

key = "your/folder/path/inside/bucket/file.csv.gzip"
obj_s3.save_csv(bucket=bucket, key=key, df=df, compression="gzip", index=False)
```

#### save_json

```python
from aws_s3_ops.aws_s3_ops import S3Operations
import pandas as pd

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/file.json"
df = pd.DataFrame([['a','b'],['c', 'd']], columns=['col1', 'col2'])

obj_s3.save_json(bucket=bucket, key=key, df=df)

key = "your/folder/path/inside/bucket/file.json.gzip"
obj_s3.save_csv(bucket=bucket, key=key, df=df, compression="gzip")
```

#### download_file

```python
from aws_s3_ops.aws_s3_ops import S3Operations

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/file_to_download.random"
local_path = "path/for/file/within/local/file_downloaded.random"

obj_s3.download_file(bucket=bucket, key=key, local_path=local_path)
```

#### upload_file

```python
from aws_s3_ops.aws_s3_ops import S3Operations

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/file_uploaded.random"
local_path = "path/for/file/within/local/file_to_upload.random"

obj_s3.upload_file(bucket=bucket, key=key, local_path=local_path)
```

#### key_exists

```python
from aws_s3_ops.aws_s3_ops import S3Operations

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/file_exists.random"

file_existence_boolean = obj_s3.key_exists(bucket=bucket, key=key)
```

#### delete_data

```python
from aws_s3_ops.aws_s3_ops import S3Operations

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/file_to_delete.random"

obj_s3.delete_data(bucket=bucket, key=key)

key = "your/folder/path/inside/bucket/folder_to_delete"

obj_s3.delete_data(bucket=bucket, key=key)
```

#### get_prefix_object

```python
from aws_s3_ops.aws_s3_ops import S3Operations

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/"

# List of all folders and files within the folder
keys = obj_s3.get_prefix_object(bucket=bucket, key=key)

# List of all folders and files within the folder with the given extension
keys = obj_s3.get_prefix_object(bucket=bucket, key=key, file_extension="txt")
```

#### get_file_buffer
```python
from aws_s3_ops.aws_s3_ops import S3Operations
import pandas as pd

obj_s3 = S3Operations()
bucket = "your-bucket-name-here"
key = "your/folder/path/inside/bucket/file.txt"

# This object can then be read using pandas or simple python file operations
buf = obj_s3.get_file_buffer(bucket=bucket, key=key)

pd.read_csv(buf)
```