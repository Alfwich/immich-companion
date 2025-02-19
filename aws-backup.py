import boto3
import sys
import os
import tempfile
import fnmatch
import zipfile
import botocore
import json
import hashlib
from pathlib import Path

from datetime import datetime
import datetime as dt

import botocore.exceptions

debug_mode = True

archive_version = 0.1

# Archives this big will get immediatlly locked and uploaded to deep storage
archive_size_threshold = 1024 * 1024 * 1024 * 10  # 10 GiB

# Staged archives this old and at least this big will get put into deep storage
archive_age_threshold = 86400 * 30  # 180 days
archive_age_and_size_threshold = 1024 * 1024 * 100  # 100 MiB

archive_age_threshold = 10  # 180 days
archive_age_and_size_threshold = 1024 * 1024 * 10  # 100 MiB

log_enabled = True
upload_enabled = False
purge_enabled = False

# DANGER: This will delete the archive info file and destroy all information about the archive
purge_archive_info = False

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
        # Ensure this works before taking the saftey off
        # s3_resource.Object(bucket_name, location).put(Body=open(file_name, 'rb'), StorageClass="DEEP_ARCHIVE")


def gen_hash_for_file(asset_path):
    hasher = hashlib.md5()
    with open(asset_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()


def add_dynamic_data_to_archive_info(archive_info):
    archive_info["last_updated"] = datetime.now().isoformat()
    archive_info["_object_set"] = set()
    archive_info["_object_asset_map"] = {}

    # Add all the objects that are in the archive in a set for easy lookup
    for archive in archive_info["archives"]:
        for k in archive["assets"].keys():
            archive_info["_object_set"].add(k)

    # There can only be one archive that can be unlocked in the info
    # Find this archive and store its identifer as this will
    # be the staged archive that we will be adding assets to
    for archive in archive_info["archives"]:
        if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED:
            archive_info["_unlocked_archive_id"] = archive["archive_id"]
            break


def load_archive_info_from_bucket(bucket_name):
    s3_client = boto3.client("s3")
    archive_info = {
        "version": archive_version,
        "num_objects_tracked": 0,
        "archives": [],
    }

    if not purge_archive_info:
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key="archive.json")
            archive_info = json.loads(response["Body"].read())
        except botocore.exceptions.ClientError as e:
            # If the file does not exist, we will create a new one
            pass

    # Add dynamic data in the archive that is used for processing
    add_dynamic_data_to_archive_info(archive_info)

    return archive_info


def save_archive_info_to_bucket(bucket_name, archive_info):

    num_objects_tracked = len(archive_info["_object_asset_map"])

    # Roughly how many objects we are tracking at this moment in time
    archive_info["num_objects_tracked"] = num_objects_tracked

    # Ensure we delete any internal keys as this data is duplicated in the archive
    archive_keys = list(archive_info.keys())
    for k in archive_keys:
        if k.startswith("_"):
            del archive_info[k]

    if debug_mode or num_objects_tracked == archive_info["num_objects_tracked"]:
        log("No new objects to add to the archive")
    else:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket_name, Key="archive.json", Body=json.dumps(archive_info))

    if debug_mode:
        with open("archive.debug.json", "w+") as f:
            json.dump(archive_info, f, indent=4)


def add_asset_to_archive_info(archive_info, asset_path, object_key):
    asset_hash = gen_hash_for_file(asset_path)

    archive = get_next_archive_from_archive_info(archive_info)
    archive_info["_object_asset_map"][asset_hash] = asset_path

    if asset_hash in archive_info["_object_set"]:
        log(f"Skipping asset {asset_path}, already exists in archive")
    else:
        log(f"Adding asset {asset_path} to archive")
        archive_info["_object_set"].add(asset_hash)

        archive["assets"][asset_hash] = {
            "object_key": object_key,
            "size": os.path.getsize(asset_path),
        }

        # Ensure this archive is unlocked and not empty
        if archive["status"] == ARCHIVE_ASSET_STATUS_EMPTY:
            archive["status"] = ARCHIVE_ASSET_STATUS_UNLOCKED
            archive["unlock_date"] = datetime.now(dt.UTC).timestamp()

        archive["archive_size"] += archive["assets"][asset_hash]["size"]

        # If the archive is too big, lock it and prepare
        if archive["archive_size"] > archive_size_threshold:
            archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE


ARCHIVE_ASSET_STATUS_EMPTY = 0
ARCHIVE_ASSET_STATUS_UNLOCKED = 1
ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE = 2
ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN = 3


def get_next_archive_from_archive_info(archive_info):
    for archive in archive_info["archives"]:
        if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED:
            # If the archive is old enough and big enough, we should lock it
            if datetime.now(dt.UTC).timestamp() - archive["unlock_date"] >= archive_age_threshold and archive["archive_size"] >= archive_age_and_size_threshold:
                archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE
            else:
                return archive
        # If the archive is empty, we can use it
        elif archive["status"] == ARCHIVE_ASSET_STATUS_EMPTY:
            return archive

    info = {
        "version": archive_version,
        "archive_id": len(archive_info["archives"]),
        "unlock_date": 0,
        "assets": {},
        "archive_size": 0,
        "status": ARCHIVE_ASSET_STATUS_EMPTY,
    }

    archive_info["archives"].append(info)

    return info


def main(aws_bucket_name, source_dir):

    for immich_working_dir in immich_working_dirs:
        if not os.path.exists(f"{source_dir}/{immich_working_dir}"):
            log(f"Working directory does not have the required folder structure: {immich_working_dirs}")
            exit()

    bucket_objects = list_bucket(aws_bucket_name)
    bucket_keys = set(map(lambda x: x.key, bucket_objects))
    disk_objects = set()

    archive_info = load_archive_info_from_bucket(aws_bucket_name)

    log(f"source_dir: {source_dir}")

    for folder in immich_working_dirs:
        for root, dir, files in os.walk(f"{source_dir}/{folder}"):
            for file_name in fnmatch.filter(files, "*"):
                asset_path = f"{root}/{file_name}"
                object_key = asset_path.removeprefix(f"{source_dir}/")
                disk_objects.add(object_key)
                add_asset_to_archive_info(archive_info, asset_path, object_key)

    log(f"Archive info: {archive_info}")

    # Check to see if we have a archive that is locked and pending freeze
    # If we do, we should freeze it and upload it to deep storage
    # along with deleting the staged raw files
    staged_archive_id = archive_info.get("_unlocked_archive_id", None)
    should_purge_staged_files = staged_archive_id is not None and archive_info["archives"][staged_archive_id]["status"] == ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE

    # For each archive that is locked and pending freeze, we will create an archive and upload it to deep storage
    for archive in archive_info["archives"]:
        if archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE:
            log(f"Freezing archive {archive['archive_id']}")
            archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN

            # Upload the archive to the bucket
            with tempfile.TemporaryDirectory() as tmpdirname:
                archive_zip = zipfile.ZipFile(f"{tmpdirname}/archive.zip", "w")
                for asset_hash, asset in archive["assets"].items():
                    asset_path = archive_info["_object_asset_map"][asset_hash]
                    if not debug_mode:
                        archive_zip.write(asset_path, asset["object_key"])
                archive_zip.close()

                # TODO(aw): This should be a deep freeze
                upload_file_aws_object(f"{tmpdirname}/archive.zip", aws_bucket_name, f"archive/archive-{archive['archive_id']}.zip")

    # Purge the staged files if we uploaded the pending archive
    if should_purge_staged_files:
        for obj in bucket_objects:
            if obj.key.startswith(f"stage/"):
                log(f"Deleting staged object {obj.key}")
                obj.delete()

    for archive in archive_info["archives"]:
        # If there is an unlocked archive with assets, we should ensure the files are uploaded
        if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED:
            for asset in archive["assets"]:
                object_key = archive["assets"][asset]["object_key"]
                stage_key = f"stage/{object_key}"
                # Upload any missing assets, if needed
                if stage_key not in bucket_keys:
                    if asset not in archive_info["_object_asset_map"]:
                        log(f"Could not find asset {asset['object_key']} in the asset map")
                        continue

                    upload_file_aws_object(archive_info["_object_asset_map"][asset], aws_bucket_name, stage_key)

    # TODO(AW) How do we deal with archives that contain deleted objects?

    save_archive_info_to_bucket(aws_bucket_name, archive_info)


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
