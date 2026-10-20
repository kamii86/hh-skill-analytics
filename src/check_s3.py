import boto3

from src.config import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    SOURCE_KEY,
)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name="us-east-1",
)

print("Проверяем подключение к RustFS...")

response = s3.head_object(
    Bucket=S3_BUCKET,
    Key=SOURCE_KEY,
)

print("Подключение успешно!")
print("Размер файла:", response["ContentLength"], "байт")

print("\nЧитаем первые 500 байт непосредственно из RustFS:")

result = s3.get_object(
    Bucket=S3_BUCKET,
    Key=SOURCE_KEY,
    Range="bytes=0-499",
)

data = result["Body"].read()

print(data.decode("utf-8", errors="replace"))
