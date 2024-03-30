import pandas as pd
from dotenv import load_dotenv
import os
import boto3

env = load_dotenv()

# s3 = boto3.resource('s3',
#                     aws_access_key_id=os.getenv("access_key"),
#                     aws_secret_access_key=os.getenv("secret_access_key"))
# for bucket in s3.buckets.all():
#         print(bucket.name)

client = boto3.client('s3',
                    aws_access_key_id=os.getenv("access_key"),
                    aws_secret_access_key=os.getenv("secret_access_key"))

response = client.get_object(Bucket="bluelambdaproject",
                             Key="newdata/use/prac/ratings.csv")

status = response.get('ResponseMetadata', {}).get('HTTPStatusCode', {})


if status == 200:
    print(f"Successfully pulled the data: Status - {status}")
    df = pd.read_csv(response.get("Body"))
    print(df.head())
else:
    print(f"Unable to pull the data: status - {status}")


