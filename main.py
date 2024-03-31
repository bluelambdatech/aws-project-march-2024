from helper.aws_helper import S3Connection

bucket = "redshift-training"
key = "classSession-373ad2f0-d334-44bb-832a-7a2fe7c00990.csv"
conn = S3Connection.create_conn(bucket=bucket)
conn.read_file(key=key)
