Bumblebee
--
Bumblebee will help you read data from Hive and check the type of columns then tranfer it to other data types.

## Installation

#### prerequisites

* python3.5 or above
* spark 2.1.1 or above

#### build package

`git clone http://this-repo/bumblebee`

`cd bumblebee`

`virtualenv .`

`. bin/activate`

`pip install -r requirments.txt`

`make build`

## Usage

#### Quick Start

* Spark Submit(**Recommendation**)

Use local spark-submit to reach the original Hive. Ensure that `hive-site.xml` is in `$SPARK_HOME/conf` 

`spark-submit --py-files dist/bumblebee.zip rollout.py --schema_path ../dbname.tablename.json 
--target_path file:///tmp/data/ --schema_parser='bq' --condition 'dt = "2016-01-01"'`

#### Arguments of rollout.py

```
usage: rollout.py [-h] --schema_path ~/path/db.name.json --target_path
                  s3://bucket/folder/ [--src_type hive] [--schema_mapper bq]
                  [--schema_parser bq] [--target_type json]
                  [--condition dt > "2018-01-01"] [--check_sum True]

required arguments:
  --schema_path ~/path/db.name.json
  --target_path s3://bucket/folder/

optional arguments:
  -h, --help            show this help message and exit
  --src_type hive
  --schema_mapper bq
  --schema_parser bq
  --target_type json
  --condition dt > "2018-01-01"
  --check_sum True
```


#### Sample of Schema


* Simple

```json
{ "col_string":"STRING",
  "col_integer":"INTEGER",
  "col_float":"FLOAT",
  "col_date":"DATE",
  "col_datetime": "DATETIME",
  "col_boolean": "BOOLEAN"}
```

* Big Query

**name** and **type** is required

```json
[{"description": "create_at", "mode": "NULLABLE", "name": "create_at", "type": "STRING"},
  {"description": "action_type", "mode": "NULLABLE", "name": "action_type", "type": "STRING"},
  {"description": "ad_id", "mode": "NULLABLE", "name": "ad_id", "type": "STRING"},
  {"description": "cell_id", "mode": "NULLABLE", "name": "cell_id", "type": "STRING"},
  {"description": "cost_points", "mode": "NULLABLE", "name": "cost_points", "type": "FLOAT"}]
```