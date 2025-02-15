import boto3
import sys
import os
import tempfile
import fnmatch
import zipfile
import time
from pathlib import Path

from datetime import datetime


archive_size_threshold = 1024 * 1024 * 1024 * 50  # 50 GiB
log_enabled = True
upload_enabled = False
purge_enabled = True

# We should find all these directories when doing the backup upload
# All other folders are generated and should be regenerated on a rebuild
immich_working_dirs = ["backups", "upload"]


def enable_log():
    global log_enabled
    log_enabled = True


def disable_log():
    global log_enabled
    log_enabled = False


def log(msg):
    global log_enabled
    if log_enabled:
        print(msg)


def list_bucket(bucket_name):
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    return list(bucket.objects.all())


def upload_file_aws_object(file_name, bucket_name, location):
    log(f"Upload file_name: {file_name}, to bucket_name: {bucket_name}, to location: {location}")
    if upload_enabled:
        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket_name, location).put(Body=open(file_name, 'rb'), StorageClass="STANDARD")


def main(aws_bucket_name, source_dir):

    for immich_working_dir in immich_working_dirs:
        if not os.path.exists(f"{source_dir}/{immich_working_dir}"):
            log(f"Working directory does not have the required folder structure: {immich_working_dirs}")
            exit()

    for obj in list_bucket(aws_bucket_name):
        # TODO(aw): Clear out files older than 180 days
        print(obj)

    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d.%H-%M-%S")
    archive_prefix = f"{timestamp}"

    # Pack the source directory into multiple archives by 'archive_size_threshold'
    with tempfile.TemporaryDirectory() as working_dir:
        current_asset_size_b = 0
        current_archive_count = 0

        log(f"source_dir: {source_dir}, working_dir: {working_dir}, archive_prefix:{archive_prefix}")
        # upload_file_aws_object(test_file, aws_bucket_name, archive_key)

        # open the first asset .zip file for the archive
        archive_name = f"{current_archive_count}.zip"
        archive_file_path = f"{working_dir}/{archive_name}"
        archive_file = zipfile.ZipFile(archive_file_path, "w")
        current_archive_count += 1

        for folder in immich_working_dirs:
            for root, dir, files in os.walk(f"{source_dir}/{folder}"):
                for file_name in fnmatch.filter(files, "*"):
                    asset_path = f"{root}/{file_name}"
                    asset_size_in_bytes = os.path.getsize(asset_path)
                    log(f"Found file asset_path: {asset_path}, size_bytes: {asset_size_in_bytes}")
                    current_asset_size_b += asset_size_in_bytes
                    archive_file.write(asset_path, asset_path)

                    if current_asset_size_b > archive_size_threshold:
                        current_asset_size_b = 0
                        archive_file.close()
                        upload_file_aws_object(archive_file_path, aws_bucket_name, f"{archive_prefix}/{archive_name}")
                        os.remove(archive_file_path)

                        # New archive
                        archive_name = f"{current_archive_count}.zip"
                        archive_file_path = f"{working_dir}/{archive_name}"
                        archive_file = zipfile.ZipFile(archive_file_path, "w")
                        current_archive_count += 1

        archive_file.close()

        # Finalize the archive, if needed
        if current_asset_size_b > 0:
            upload_file_aws_object(archive_file_path, aws_bucket_name, f"{archive_prefix}/{archive_name}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        log("Expected usage: python3.exe aws-backup.py <aws_bucket_name> <source_dir>")
        exit()

    if not os.path.isdir(f"{Path.home()}/.aws"):
        log("Did not find ~/.aws configuration folder. Please setup AWS's auth files.")
        exit()

    aws_bucket_name = sys.argv[1]
    source_dir = sys.argv[2]

    main(aws_bucket_name, source_dir)
