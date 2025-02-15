import boto3

from datetime import datetime

bucket_name = "wuterich-image-storage"


def main():
    """
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_buckets")
    response_iterator = paginator.paginate(
        PaginationConfig={
            "PageSize": 50,
            "StartingToken": None
        }
    )

    for page in response_iterator:
        if "Buckets" in page and page["Buckets"]:
            for b in page["Buckets"]:
                print(f"{b['Name']}")
    """

    s3_resource = boto3.resource("s3")

    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.all():
        print(obj)

    now = datetime.now()
    test_file = "test.zip"
    timestamp = now.strftime("%Y-%m-%d.%H-%M-%S")
    archive_key = f"{timestamp}/{test_file}"

    s3_resource.Object(bucket_name, archive_key).put(Body=open(test_file, 'rb'), StorageClass="STANDARD")


if __name__ == "__main__":
    main()
