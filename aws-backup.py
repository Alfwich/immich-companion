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

archive_version = 0.1

# Archives this big will get immediatlly locked and uploaded to deep storage
archive_size_threshold = 1024 * 1024 * 1024 * 5  # 5 GiB

# Unlocked archives will be locked and uploaded to deep storage after 30 days if its over 1 GiB
archive_age_threshold = 86400 * 30  # 30 days
archive_age_and_size_threshold = 1024 * 1024 * 1024  # 1 GiB

log_enabled = True

# If enabled, files will be uploaded to the cloud
upload_enabled = False

# TODO(aw): This should provide a way to prune archives that have a lot of deleted assets
purge_enabled = False

# DANGER: This will delete the archive info file and destroy all information about the archive
purge_archive_info = False

# We should find all these directories when doing the backup upload
# All other folders are generated and should be regenerated on a rebuild
immich_working_dirs = ["backups", "upload"]

agent_state_file_name = "agent-state.json"
archive_state_file_name = "archive.json"

log_file_name = f"backup-log.{datetime.now().timestamp()}.txt"
log_file = open(f"{os.path.dirname(os.path.realpath(__file__))}/{log_file_name}", "w+")


def enable_log():
    global log_enabled
    log_enabled = True


def disable_log():
    global log_enabled
    log_enabled = False


def log(msg):
    global log_enabled
    if log_enabled:
        log_msg = f"[{datetime.now().isoformat()}] {msg}"
        print(log_msg)
        if not log_file.closed:
            log_file.write(f"{log_msg}\n")


def list_bucket(bucket_name):
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    return list(bucket.objects.all())


def upload_file_aws_object(file_name, bucket_name, location):
    log(f"  Upload file_name: {file_name}, to bucket_name: {bucket_name}, to location: {location}")
    if upload_enabled:
        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket_name, location).put(Body=open(file_name, 'rb'), StorageClass="STANDARD")
        # Ensure this works before taking the saftey off
        # s3_resource.Object(bucket_name, location).put(Body=open(file_name, 'rb'), StorageClass="DEEP_ARCHIVE")


def hash_string_list(l):
    if len(l) == 0:
        return ""

    encoded = "".join(sorted(l)).encode("utf-8")
    hash = hashlib.sha256(encoded)
    return hash.hexdigest()


def get_hash_for_archive_info(archive_info):
    return hash_string_list(list(archive_info["assets"].keys()) + list(map(lambda x: f"archive-{x['archive_id']}:{x['status']}", archive_info["archives"])))


def new_archive_info():
    return {
        "version": archive_version,
        "hash": "",
        "archives": [],
        "assets": {},
        "last_updated": datetime.now().isoformat(),
    }


ARCHIVE_ASSET_STATUS_EMPTY = 0
ARCHIVE_ASSET_STATUS_UNLOCKED = 1
ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE = 2
ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN = 3


def add_new_archive_data_info_to_archive_info(archive_info):
    archive_data_info = {
        "version": archive_version,
        "archive_id": str(len(archive_info["archives"])),
        "unlock_date": 0,
        "size": 0,
        "status": ARCHIVE_ASSET_STATUS_EMPTY,
    }
    archive_info["archives"].append(archive_data_info)
    return archive_data_info


def new_asset_info(size, archive_id):
    return {
        "archives": {
            archive_id: new_asset_info_archive_info(size)
        },
    }


def new_asset_info_archive_info(size):
    return {
        "size": size,
    }


def add_asset_to_archive_info(archive_info, asset_path, object_key):

    archive = get_next_archive_from_archive_info(archive_info)
    asset_in_archive_info = object_key in archive_info["assets"]

    disk_asset_size = os.path.getsize(asset_path)
    asset_inserted = False

    # If the asset is not in the archive_info, then its new and we should add it to the first unlocked archive
    if not asset_in_archive_info:
        log(f"    Adding asset {asset_path} to archive")
        new_asset = new_asset_info(disk_asset_size, str(archive["archive_id"]))
        archive_info["assets"][object_key] = new_asset
        archive["size"] += disk_asset_size
        asset_inserted = True
    else:
        # If the asset is in the archive_info, we should check if it has changed
        asset = archive_info["assets"][object_key]
        asset_sizes = [asset_data["size"] for asset_data in asset["archives"].values()]

        # By default, select the current open archive unless this asset already belongs to an open archive
        archive_to_update = archive

        # This asset key has a new size so it needs to be updated
        if disk_asset_size not in asset_sizes:
            for archive_id in asset["archives"]:
                _archive = archive_info["archives"][int(archive_id)]
                if _archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED:
                    archive_to_update = _archive
                    break

            asset["archives"][str(archive_to_update["archive_id"])] = new_asset_info_archive_info(disk_asset_size)
            archive_to_update["size"] += disk_asset_size
            asset_inserted = True
        else:
            log(f"    Skipping asset {asset_path}, already exists in archive and has not changed")

    # If the asset was inserted and the archive is empty, we should unlock it
    if asset_inserted and archive["status"] == ARCHIVE_ASSET_STATUS_EMPTY:
        archive["status"] = ARCHIVE_ASSET_STATUS_UNLOCKED
        archive["unlock_date"] = int(datetime.now(dt.UTC).timestamp())

    # Progress the archive status, if needed
    archive_size = archive["size"]
    is_above_size_threshold = archive_size >= archive_size_threshold
    is_archive_old = datetime.now(dt.UTC).timestamp() - archive["unlock_date"] >= archive_age_threshold
    is_old_and_above_size_threshold = is_archive_old and archive_size >= archive_age_and_size_threshold
    archive_is_unlocked = archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED

    if archive_is_unlocked and (is_above_size_threshold or is_old_and_above_size_threshold):
        archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE


def get_next_archive_from_archive_info(archive_info):
    for archive in archive_info["archives"]:
        if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED or archive["status"] == ARCHIVE_ASSET_STATUS_EMPTY:
            return archive

    # No archive is available, we should create a new one
    return add_new_archive_data_info_to_archive_info(archive_info)


def load_archive_info_from_bucket(bucket_name):
    archive_info = new_archive_info()

    s3_client = boto3.client("s3")
    if not purge_archive_info:
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=archive_state_file_name)
            archive_info = json.loads(response["Body"].read())
        except botocore.exceptions.ClientError as e:
            # If the file does not exist, we will create a new one
            pass

    return archive_info


def save_archive_info_to_bucket(bucket_name, archive_info):

    log(f"Saving archive info to bucket: {bucket_name}, archive hash: {archive_info['hash']}")
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket_name, Key=archive_state_file_name, Body=json.dumps(archive_info))

    # Save the archive_info to disk for debugging
    with open(f"{os.path.dirname(os.path.realpath(__file__))}/{archive_state_file_name}", "w+") as f:
        json.dump(archive_info, f, indent=4)


AGENT_STATE_UNKNOWN = 0
AGENT_STATE_PROCESSING = 1
AGENT_STATE_COMPLETE = 2


def save_agent_state_to_bucket(bucket_name, agent_state):

    body = {
        "version": archive_version,
        "agent_state": agent_state,
    }

    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket_name, Key=agent_state_file_name, Body=json.dumps(body))


def load_agent_state_from_bucket(bucket_name):
    agent_state = AGENT_STATE_UNKNOWN

    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=agent_state_file_name)
        agent_state = json.loads(response["Body"].read())
        agent_state = agent_state["agent_state"]
    except botocore.exceptions.ClientError as e:
        # If the file does not exist, we will create a new one
        pass

    return agent_state


def main(aws_bucket_name, source_dir):

    bucket_objects = list_bucket(aws_bucket_name)
    bucket_object_keys = [obj.key for obj in bucket_objects]
    disk_objects = {}

    agent_state = load_agent_state_from_bucket(aws_bucket_name)
    archive_info = load_archive_info_from_bucket(aws_bucket_name)

    # If the agent was never completed we should ensure archive consistency, as possible
    if agent_state != AGENT_STATE_COMPLETE:
        log(f"Agent state was processing, checking archives for consistency...")
        for archive in archive_info["archives"]:
            archive_key = f"archive/archive-{archive['archive_id']}.zip"
            archive_exists = archive_key in bucket_object_keys

            log(f"  Checking archive {archive['archive_id']} -> status: {archive['status']}")
            log(f"      Archive exists: {archive_exists}")

            # The archive was supposed to be uploaded, but it does not exist
            if archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN and not archive_exists:
                log(f"      Archive does not exist on remote storage, marking as pending upload")
                archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE

            # The archive was supposed to be uploaded and it does exist
            elif archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE and archive_exists:
                log(f"      Archive exists on remote storage, marking as uploaded")
                # Setting this status will prevent further processing as its already uploaded
                archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN

                # Update asset states that are in the archive to mark they are stored
                for asset in archive_info["assets"]:
                    if archive["archive_id"] in archive_info["assets"][asset]["archives"]:
                        archive_info["assets"][asset]["archives"][archive["archive_id"]]["status"] = ASSET_STATUS_STORED

    log(f"Walking the source directory: {source_dir}, and updating asset catalogue")
    old_archive_info_hash = archive_info["hash"]
    for folder in immich_working_dirs:
        for root, dir, files in os.walk(f"{source_dir}/{folder}"):
            for file_name in fnmatch.filter(files, "*"):
                asset_path = f"{root}/{file_name}"
                object_key = asset_path.removeprefix(f"{source_dir}/").replace("\\", "/")
                disk_objects[object_key] = asset_path
                add_asset_to_archive_info(archive_info, asset_path, object_key)

    # Update the hash for the archive
    archive_info["hash"] = get_hash_for_archive_info(archive_info)
    if old_archive_info_hash == archive_info["hash"] and not agent_state == AGENT_STATE_PROCESSING:
        log("Archive has not changed so performing no operations")

        # Save the archive_info to the bucket before we do any operations on it in-case of failure
        save_archive_info_to_bucket(aws_bucket_name, archive_info)
        save_agent_state_to_bucket(aws_bucket_name, AGENT_STATE_PROCESSING)

        # Upload assets that are fresh and are contained in an unlocked archive
        log("Checking staged assets for upload")
        for asset in archive_info["assets"]:
            should_upload = False
            asset_data = archive_info["assets"][asset]

            for archive_id, data in asset_data["archives"].items():
                archive = archive_info["archives"][int(archive_id)]

                # If the archive is unlocked and the object does not exist on cloud storage, upload it again
                if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED and f"stage/{asset}" not in bucket_object_keys:
                    should_upload = True

            # Directly upload fresh assets as they are either new or updated
            if should_upload and asset in disk_objects:
                stage_key = f"stage/{asset}"
                upload_file_aws_object(disk_objects[asset], aws_bucket_name, stage_key)

        # Prune any assets that are not staged nor are on disk
        archive_info_keys = set(archive_info["assets"].keys())
        for key in archive_info_keys:

            # If the asset is in a frozen archive we should not delete it
            in_frozen_archive = False
            for archive in archive_info["assets"][key]["archives"]:
                if archive_info["archives"][int(archive)]["status"] == ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN:
                    in_frozen_archive = True

            if not in_frozen_archive and key not in disk_objects:
                for archive_id in archive_info["assets"][key]["archives"]:
                    archive = archive_info["archives"][int(archive_id)]
                    archive["size"] -= archive_info["assets"][key]["archives"][archive_id]["size"]
                del archive_info["assets"][key]

        log("Checking archives to see if we need to upload any")
        for archive in archive_info["archives"]:
            archive_id = str(archive["archive_id"])
            if archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE:
                log(f"Uploading archive {archive['archive_id']}")

                with tempfile.TemporaryDirectory() as tmpdirname:
                    archive_zip = zipfile.ZipFile(f"{tmpdirname}/archive.zip", "w")
                    for object_key in archive_info["assets"]:
                        if archive_id in archive_info["assets"][object_key]["archives"]:
                            if object_key in disk_objects:
                                log(f"  Writing {object_key} to archive")
                                asset_path = disk_objects[object_key]
                                if upload_enabled:
                                    archive_zip.write(asset_path, object_key)
                            else:
                                log(f"  Skipping {object_key} in archive as it does not exist on the filesystem")

                    archive_zip.close()

                    upload_file_aws_object(f"{tmpdirname}/archive.zip", aws_bucket_name, f"archive/archive-{archive['archive_id']}.zip")

                archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN

        log("Deleting staged files that are not on the filesystem")
        for obj in bucket_objects:
            if obj.key.startswith("stage/"):
                object_key = obj.key.removeprefix("stage/")
                # If this staged object is not on disk, we should delete it
                if object_key not in disk_objects:
                    log(f"    Deleting staged object {obj.key}")
                    obj.delete()

                    # If the asset is in a frozen archive we should not delete its record
                    in_frozen_archive = False
                    if object_key in archive_info["assets"]:
                        for archive in archive_info["assets"][object_key]["archives"]:
                            if archive_info["archives"][int(archive)]["status"] == ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN:
                                in_frozen_archive = True

                    if object_key in archive_info["assets"]:
                        del archive_info["assets"][object_key]

                # Check to see if we have archived this asset, if so we should delete the staged object
                elif object_key in archive_info["assets"]:
                    in_unlocked_archive = False
                    for archive in archive_info["assets"][object_key]["archives"]:
                        if archive_info["archives"][int(archive)]["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED:
                            in_unlocked_archive = True

                    # If the object is not in any unlocked archive we can safely delete it
                    if not in_unlocked_archive:
                        log(f"Deleting staged object {obj.key}")
                        obj.delete()

        # True up the archive sizes for deleted keys
        for archive in archive_info["archives"]:
            archive["size"] = 0

        for asset in archive_info["assets"]:
            for archive_id, data in archive_info["assets"][asset]["archives"].items():
                archive = archive_info["archives"][int(archive_id)]
                archive["size"] += data["size"]

        # Save the archive_info at the end to ensure we have the most up to date information

        old_archive_info_hash = archive_info["hash"]
        archive_info["hash"] = get_hash_for_archive_info(archive_info)
        if old_archive_info_hash != archive_info["hash"]:
            log("Saving archive state to cloud as it was modified during processing")
            save_archive_info_to_bucket(aws_bucket_name, archive_info)

    save_agent_state_to_bucket(aws_bucket_name, AGENT_STATE_COMPLETE)

    log("Finished")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        log("Expected usage: python3.exe aws-backup.py <aws_bucket_name> <source_dir>")
        exit()

    if not os.path.isdir(f"{Path.home()}/.aws"):
        log("Did not find ~/.aws configuration folder. Please setup AWS's auth files.")
        exit()

    aws_bucket_name = sys.argv[1]
    source_dir = sys.argv[2]

    for immich_working_dir in immich_working_dirs:
        if not os.path.exists(f"{source_dir}/{immich_working_dir}"):
            log(f"Working directory does not have the required folder structure: {immich_working_dirs}")
            exit()

    main(aws_bucket_name, source_dir)
