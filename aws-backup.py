import boto3
import sys
import os
import tempfile
import fnmatch
import zipfile
import botocore
import json
import hashlib
import time
from pathlib import Path

from datetime import datetime
import datetime as dt

import botocore.exceptions

archive_version = 0.1

# Archives this big will get immediately locked and uploaded to deep storage while processing
archive_size_threshold = 1024 * 1024 * 1024 * 1  # 1 GiB

# Unlocked archives will be locked and uploaded to deep storage if:
# - The archive size is greater than 512 MiB and has been staged for at least 7 days
archive_age_threshold = 86400 * 7  # 7 days
archive_age_and_size_threshold = 1024 * 1024 * 512  # 512 MiB

# Max amount of time in seconds that the processing can run.
# Any uploads for archives or stages assets will be
# picked back up on the next run of the backup tool
max_processing_time = 60 * 60 * 22  # 22 Hours

# If enabled, files will be uploaded to the cloud
upload_enabled = True

# If we should allow storing files into deep storage
deep_storage_enabled = False

# TODO(aw): This should provide a way to prune archives that have a lot of deleted assets
#           Currently archives that have been frozen are considered immutable
purge_enabled = False

# We should find all these directories when doing the backup upload
# All other folders are generated and should be regenerated on a rebuild
immich_working_dirs = ["immich/upload"]

# Custom directory that can be used to put any file for long-term storage
custom_working_dirs = ["backup"]

# Directories that will never be put into long-term storage and will forever live in
# standard storage. These files are expected to be changing frequently and size management should
# be enforced by the processes that place files into this directory
stage_only_working_dirs = ["immich/backups"]

agent_state_file_name = "agent-state.json"
archive_state_file_name = "archive.json"

log_file_name = f"backup-log.{datetime.now().timestamp()}.txt"
log_file = open(f"{os.path.dirname(os.path.realpath(__file__))}/{log_file_name}", "w+")
sys.stderr = log_file

start_timestamp = datetime.now(dt.UTC).timestamp()

log_indent_level = 0
log_enabled = True


def push_log_indent():
    global log_indent_level
    log_indent_level += 1


def pop_log_indent():
    global log_indent_level
    if log_indent_level > 0:
        log_indent_level -= 1


def enable_log():
    global log_enabled
    log_enabled = True


def disable_log():
    global log_enabled
    log_enabled = False


def within_timelimit():
    return datetime.now(dt.UTC).timestamp() - start_timestamp <= max_processing_time


def log(msg, added_indent=0):
    global log_enabled
    if log_enabled:
        log_msg = f"[{datetime.now().isoformat()}]{'  ' * (log_indent_level + added_indent + 1)}{msg}"
        print(log_msg)
        if not log_file.closed:
            log_file.write(f"{log_msg}\n")


def log_execution_parameters():
    log(f"Execution parameters")
    push_log_indent()
    log(f"archive_version: {archive_version}")
    log(f"archive_size_threshold: {int(archive_size_threshold / 1024 / 1024)} MiB")
    log(f"archive_age_threshold: {int(archive_age_threshold / 86400)} Days")
    log(f"archive_age_and_size_threshold: {int(archive_age_and_size_threshold / 1024 / 1024)} MiB")
    log(f"max_processing_time: {int(max_processing_time / 60 / 60)} Hours")
    log(f"upload_enabled: {upload_enabled}")
    log(f"deep_storage_enabled: {deep_storage_enabled}")
    log(f"purge_enabled: {purge_enabled}")
    log(f"immich_working_dirs: {immich_working_dirs}")
    log(f"stage_only_working_dirs: {stage_only_working_dirs}")
    log(f"custom_working_dirs: {custom_working_dirs}")
    log(f"agent_state_file_name: {agent_state_file_name}")
    log(f"archive_state_file_name: {archive_state_file_name}")
    pop_log_indent()


def list_bucket(bucket_name):
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    return list(bucket.objects.all())


def upload_file_aws_object(file_name, bucket_name, location, storage_class="STANDARD"):
    log(f"Upload file_name: {file_name}, to bucket_name: {bucket_name}, to location: {location}")
    if upload_enabled:
        try:
            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket_name, location).put(Body=open(file_name, 'rb'), StorageClass=storage_class)
        except botocore.exceptions.ClientError as e:
            log(f"AWS exception: {e}")
            log("Failed to upload file. Aborting.")
            exit()


def upload_file_aws_object_standard(file_name, bucket_name, location):
    upload_file_aws_object(file_name, bucket_name, location)


def upload_file_aws_object_deep_archive(file_name, bucket_name, location):
    # If deep storage is disabled use standard storage instead of deep storage
    if deep_storage_enabled:
        upload_file_aws_object(file_name, bucket_name, location, "DEEP_ARCHIVE")
    else:
        upload_file_aws_object_standard(file_name, bucket_name, location)


def hash_string_list(l):
    if len(l) == 0:
        return ""

    encoded = "".join(sorted(l)).encode("utf-8")
    hash = hashlib.sha256(encoded)
    return hash.hexdigest()


def get_asset_hash(asset_key, asset):
    asset_archive_keys = [asset_key]
    for id, value in asset["archives"].items():
        asset_archive_keys.append(f"{id}-{value['size']}")
    return ":".join(asset_archive_keys)


def get_hash_for_archive_info(archive_info):
    asset_keys = list(map(lambda x: get_asset_hash(x[0], x[1]), archive_info["assets"].items()))
    archive_keys = list(map(lambda x: f"archive-{x['archive_id']}:{x['status']}", archive_info["archives"]))

    return hash_string_list(asset_keys + archive_keys)


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
        log(f"Adding asset {asset_path} to archive")
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

    # If the asset was inserted and the archive is empty, we should unlock it
    if asset_inserted and archive["status"] == ARCHIVE_ASSET_STATUS_EMPTY:
        archive["status"] = ARCHIVE_ASSET_STATUS_UNLOCKED
        archive["unlock_date"] = int(datetime.now(dt.UTC).timestamp())

    # Progress the archive status, if needed
    archive_size = archive["size"]
    archive_is_unlocked = archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED
    is_above_size_threshold = archive_size >= archive_size_threshold
    is_archive_old = datetime.now(dt.UTC).timestamp() - archive["unlock_date"] >= archive_age_threshold
    is_old_and_above_size_threshold = is_archive_old and archive_size >= archive_age_and_size_threshold

    # These cases are where we move an unlocked archive into frozen storage
    # - Archive size is above the max archive size, we immediatly move this into long-term storage
    # - Archive is old enough, and within a reasonable size, that we should move it into long-term storage
    if archive_is_unlocked and (is_above_size_threshold or is_old_and_above_size_threshold):
        log(f"Locking archive id: {archive['archive_id']}, size: {int(archive['size'] / 1024 / 1024)} MiB, as its over {int(archive_size_threshold / 1024 / 1024)} MiB")
        archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE


def get_next_archive_from_archive_info(archive_info):
    for archive in archive_info["archives"]:
        if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED or archive["status"] == ARCHIVE_ASSET_STATUS_EMPTY:
            return archive

    # No archive is available, we should create a new one
    return add_new_archive_data_info_to_archive_info(archive_info)


def count_number_of_assets_in_archive(archive_info, archive_id):
    num_objects = 0

    for archive in archive_info["archives"]:
        if archive["archive_id"] == archive_id:
            for asset in archive_info["assets"].values():
                if archive["archive_id"] in asset["archives"]:
                    num_objects += 1
            break

    return num_objects


def load_archive_info_from_bucket(bucket_name):

    log(f"Loading archive info from bucket: {bucket_name}")
    push_log_indent()

    archive_info = new_archive_info()
    s3_client = boto3.client("s3")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=archive_state_file_name)
        archive_info = json.loads(response["Body"].read())
    except botocore.exceptions.ClientError as e:
        log(f"AWS exception: {e}")
        log("Failed to pull down archive state information from cloud storage. Aborting.")
        exit()

    total_backup_size = 0
    for archive in archive_info["archives"]:
        total_backup_size += archive["size"]
        if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED:
            num_objects = count_number_of_assets_in_archive(archive_info, archive["archive_id"])
            log(f"Current open archive id: {archive['archive_id']}, unlock_date: {archive['unlock_date']}, size: {int(archive['size']/1024/1024)} MiB, num_objects: {num_objects}")

            unlock_date = int(archive['unlock_date'])
            if unlock_date > 0:
                archive_unlock_age = int(datetime.now(dt.UTC).timestamp()) - unlock_date
                time_until_unlock = archive_age_threshold - archive_unlock_age
                time_until_unlock_message = "NOW" if time_until_unlock <= 0 else f"{(time_until_unlock / 60 / 60):.2f} hr"
                log(f"Time until archive lock and archive: {time_until_unlock_message}", 1)

    log(f"Total number of objects stored: {len(archive_info['assets'])}, total archives in backup: {len(archive_info['archives'])}, total size: {int(total_backup_size / 1024 / 1024)} MiB")

    pop_log_indent()

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


def upload_stage_only_assets(stage_only_assets, aws_bucket_name):
    log("Checking stage-only artifacts for changes")

    push_log_indent()
    for asset in stage_only_assets:
        asset_path = asset[0]
        stage_key = asset[1]

        if within_timelimit():
            upload_file_aws_object_standard(asset_path, aws_bucket_name, stage_key)
    pop_log_indent()


def update_archive_size_for_assets(archive_info):
    for archive in archive_info["archives"]:
        archive["size"] = 0

    for asset in archive_info["assets"]:
        for archive_id, data in archive_info["assets"][asset]["archives"].items():
            archive = archive_info["archives"][int(archive_id)]
            archive["size"] += data["size"]

    # If any of the archives are empty, we should mark it as empty and remove the unlock_date
    for archive in archive_info["archives"]:
        if archive["size"] == 0 and archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED or archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE:
            archive["status"] = ARCHIVE_ASSET_STATUS_EMPTY
            archive["unlock_date"] = 0


def main(aws_bucket_name, backup_dir):

    bucket_objects = list_bucket(aws_bucket_name)
    bucket_object_keys = [obj.key for obj in bucket_objects]
    disk_objects = {}

    log_execution_parameters()

    agent_state = load_agent_state_from_bucket(aws_bucket_name)
    archive_info = load_archive_info_from_bucket(aws_bucket_name)

    # If the agent was never completed we should ensure archive consistency, as possible
    if agent_state != AGENT_STATE_COMPLETE:
        log(f"Agent state was processing, or unknown, checking archives for consistency...")
        push_log_indent()

        for archive in archive_info["archives"]:
            archive_key = f"archive/archive-{archive['archive_id']}.zip"
            archive_exists = archive_key in bucket_object_keys

            log(f"Checking archive {archive['archive_id']} -> status: {archive['status']}")
            log(f"Archive exists: {archive_exists}", 1)

            # The archive was supposed to be uploaded, but it does not exist
            if archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN and not archive_exists:
                log(f"Archive does not exist on remote storage, marking as pending upload")
                archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE

            # The archive was supposed to be uploaded and it does exist
            elif archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE and archive_exists:
                log(f"Archive exists on remote storage, marking as uploaded")
                # Setting this status will prevent further processing as its already uploaded
                archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN

        pop_log_indent()

    log(f"Walking the source directory: {backup_dir}, and updating asset catalogue")
    push_log_indent()
    old_archive_info_hash = archive_info["hash"]
    num_files_discovered = 0
    walk_timestamp = time.time()
    for folder in map(lambda x: f"{backup_dir}/{x}", immich_working_dirs + custom_working_dirs):
        if os.path.exists(folder):
            for root, dir, files in os.walk(folder):
                for file_name in fnmatch.filter(files, "*"):
                    num_files_discovered += 1
                    asset_path = f"{root}/{file_name}"
                    object_key = asset_path.removeprefix(f"{backup_dir}/").replace("\\", "/")
                    disk_objects[object_key] = asset_path
                    add_asset_to_archive_info(archive_info, asset_path, object_key)

                    new_timestamp = time.time()
                    if (new_timestamp - walk_timestamp > 5):
                        log(f"Still scanning while finding {num_files_discovered} files ...", 1)
                        walk_timestamp = new_timestamp

    log(f"Finished with {num_files_discovered} files in the standard archive working dirs")

    # Walk the stage-only directory for files that bypass the archive system
    stage_only_assets_to_upload = []
    stage_only_assets_num = 0
    stage_only_assets_size = 0
    for folder in map(lambda x: f"{backup_dir}/{x}", stage_only_working_dirs):
        if os.path.exists(folder):
            for root, dir, files in os.walk(folder):
                for file_name in fnmatch.filter(files, "*"):
                    asset_path = f"{root}/{file_name}"
                    object_key = asset_path.removeprefix(f"{backup_dir}/").replace("\\", "/")
                    disk_objects[object_key] = asset_path
                    stage_key = f"stage/{object_key}"

                    stage_only_assets_size += os.path.getsize(asset_path)
                    stage_only_assets_num += 1

                    if stage_key not in bucket_object_keys:
                        stage_only_assets_to_upload.append((asset_path, stage_key))
    log(f"Found {stage_only_assets_num} files in the stage-only working dirs, of size: {int(stage_only_assets_size / 1024 / 1024)} MiB")

    pop_log_indent()

    # Update the hash for the archive
    archive_info["hash"] = get_hash_for_archive_info(archive_info)
    if old_archive_info_hash == archive_info["hash"] and not agent_state == AGENT_STATE_PROCESSING:
        # Ensure that the stage-only assets are uploaded even if the rest of the archive is good
        upload_stage_only_assets(stage_only_assets_to_upload, aws_bucket_name)
        log("Archive has not changed so performing no operations")
        log("Finished")
        return

    # Save the archive_info to the bucket before we do any operations on it in-case of failure
    save_archive_info_to_bucket(aws_bucket_name, archive_info)
    save_agent_state_to_bucket(aws_bucket_name, AGENT_STATE_PROCESSING)

    upload_stage_only_assets(stage_only_assets_to_upload, aws_bucket_name)

    log("Checking staged assets for upload")
    push_log_indent()
    for asset in archive_info["assets"]:
        should_upload = False
        asset_data = archive_info["assets"][asset]

        for archive_id, data in asset_data["archives"].items():
            archive = archive_info["archives"][int(archive_id)]

            # If the archive is unlocked and the object does not exist on cloud storage, upload it again
            if archive["status"] == ARCHIVE_ASSET_STATUS_UNLOCKED and f"stage/{asset}" not in bucket_object_keys:
                should_upload = True

        # Directly upload fresh assets as they are either new or updated and we have time
        if should_upload and asset in disk_objects and within_timelimit():
            stage_key = f"stage/{asset}"
            upload_file_aws_object_standard(disk_objects[asset], aws_bucket_name, stage_key)
    pop_log_indent()

    log("Pruning assets which have been deleted from the archive")
    push_log_indent()
    archive_info_keys = set(archive_info["assets"].keys())
    for key in archive_info_keys:

        # If the asset is in a frozen archive we should not delete it
        in_frozen_archive = False
        for archive in archive_info["assets"][key]["archives"]:
            if archive_info["archives"][int(archive)]["status"] == ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN:
                in_frozen_archive = True

        if not in_frozen_archive and key not in disk_objects:
            log(f"Deleting asset: {key}")
            for archive_id in archive_info["assets"][key]["archives"]:
                archive = archive_info["archives"][int(archive_id)]
                archive["size"] -= archive_info["assets"][key]["archives"][archive_id]["size"]
            del archive_info["assets"][key]
    pop_log_indent()

    log("Checking archives to see if we need to upload any")
    push_log_indent()
    for archive in archive_info["archives"]:
        archive_id = str(archive["archive_id"])

        # If the archive is pending freeze, and we have time, process the archive and upload it
        if archive["status"] == ARCHIVE_ASSET_STATUS_LOCKED_PENDING_FREEZE and within_timelimit():
            log(f"Uploading archive {archive['archive_id']}")

            push_log_indent()
            with tempfile.TemporaryDirectory() as tmpdirname:
                try:
                    archive_zip = zipfile.ZipFile(f"{tmpdirname}/archive.zip", "w", strict_timestamps=False)
                    for object_key in archive_info["assets"]:
                        if archive_id in archive_info["assets"][object_key]["archives"]:
                            if object_key in disk_objects:
                                log(f"Writing {object_key} to archive")
                                asset_path = disk_objects[object_key]
                                if upload_enabled:
                                    archive_zip.write(asset_path, object_key)
                            else:
                                log(f"Skipping {object_key} in archive as it does not exist on the filesystem")

                    archive_zip.close()

                    upload_file_aws_object_deep_archive(f"{tmpdirname}/archive.zip", aws_bucket_name, f"archive/archive-{archive['archive_id']}.zip")

                finally:
                    archive_zip.close()
            pop_log_indent()

            archive["status"] = ARCHIVE_ASSET_STATUS_LOCKED_UPLOADED_FROZEN
    pop_log_indent()

    log("Deleting staged files that are not on the filesystem")
    push_log_indent()
    for obj in bucket_objects:
        if obj.key.startswith("stage/"):
            object_key = obj.key.removeprefix("stage/")
            # If this staged object is not on disk, we should delete it
            if object_key not in disk_objects:
                log(f"Deleting staged object {obj.key}")
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
    pop_log_indent()

    # True up the archive sizes for deleted keys
    update_archive_size_for_assets(archive_info)

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
        log("Expected usage: python3.exe aws-backup.py <aws-bucket-name> <backup-dir>")
        exit()

    if not os.path.isdir(f"{Path.home()}/.aws"):
        log("Did not find ~/.aws configuration folder. Please setup AWS's auth files.")
        exit()

    aws_bucket_name = sys.argv[1]
    backup_dir = sys.argv[2]

    for immich_working_dir in immich_working_dirs:
        if not os.path.exists(f"{backup_dir}/{immich_working_dir}"):
            log(f"Backup directory does not have the required immich folder structure: {immich_working_dirs}")
            exit()

    try:
        main(aws_bucket_name, backup_dir)
    except Exception as e:
        log(f"Unhandled exception caught: {e}")
        raise
