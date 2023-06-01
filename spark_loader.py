from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import yaml

USER_PATH = "/Users/emiliano.lara/Documents/Personal/Globant/"
YAML_PATH = USER_PATH + "globant_challenge_emiliano_lara/db.yaml"

def get_schema(file_name):
    if file_name == 'jobs':
        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('job', StringType(), True)
        ])
    elif file_name == 'hired_employees':
        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('datetime', StringType(), True),
            StructField('department_id', IntegerType(), True),
            StructField('job_id', IntegerType(), True)
        ])
    elif file_name == 'departments':
        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('department', StringType(), True)
        ])
    else:
        schema = None

    return schema

def get_db_properties(path):
    with open(path, "r") as stream:
        try:
            yml_props = yaml.safe_load(stream)
            return yml_props
        except yaml.YAMLError as exc:
            print(exc)

spark = SparkSession.builder.appName('Globant Challenge').master('local[*]').getOrCreate()
path = sys.argv[1]
table_name = sys.argv[2]
schema = get_schema(table_name)

if schema is not None: 
    df = spark.read.format('csv').schema(schema).load(path)
    props = get_db_properties(YAML_PATH)
    url = 'jdbc:mysql://{}:{}/{}'.format(props['host'], props['port'], props['dbname'])
    properties = {
        'user': props['user'],
        'password': props['password'],
        'driver': 'com.mysql.jdbc.Driver'
    }

    df.write.jdbc(url=url, table=table_name, mode='overwrite', properties=properties)
    spark.stop()

