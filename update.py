import pyexiv2
import sys
import os
import shutil
import fnmatch
import json
import re
from datetime import datetime

if os.name == "nt":
    from win32_setctime import setctime

files_to_consider = ["bmp", "gif", "jpg", "jpeg", "png", "mp4", "webp", "mov"]

exif_metadata_keys_to_consider = ["Exif.Photo.DateTimeDigitized", "Exif.Photo.DateTimeOriginal"]
exif_metadata_datetime_format = "%Y:%m:%d %H:%M:%S"

# Fallback image date that puts assets in the past so that can be grouped
fallback_image_date = datetime(2010, 1, 1)

# Any image paths that fail to find the date will be stored here
image_misses = []
log_enabled = True

# Output files will be batched into folder sets no bigger than this value
max_output_dir_file_limit = 4000

# Once the tool is finished it will output result metadata to this file
output_info_file = "info.json"


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


def try_get_google_metadata(img_path):
    metadata_path = f"{img_path}.supplemental-metadata.json"
    if os.path.exists(metadata_path):
        with open(metadata_path, "r") as json_raw:
            return json.load(json_raw)

    return {}


google_photos_year_hint = "Photos from "


date_format_expressions = [
    {  # Simple format that is just 8 numbers in a row, likely a date
        "r": "([0-9]{8})",
        "f": "%Y%m%d"
    },
    {
        "r": "([0-9]{4}-[0-9]{2}-[0-9]{2})",
        "f": "%Y-%m-%d"
    },
]


def try_get_date_information_from_path(img_path):
    # Check filename for date information
    log(f"Trying to get date information from img path: {img_path}")

    # Start with 1971, 1, 1 as this works with Windows and is earlier than any pictures that will be processed
    base_datetime = datetime(1971, 1, 1)
    working_datetime = datetime(1971, 1, 1)

    # Google gives us a hint here about the year, at minumum
    if google_photos_year_hint in img_path:
        parts = img_path.split(google_photos_year_hint)
        if len(parts) > 1:
            year = int(parts[1].split("/")[0])
            working_datetime = working_datetime.replace(year=year)

    # Next look at the filename and check a few known formats
    filename = os.path.basename(img_path)
    for exp in date_format_expressions:
        hits = re.search(exp["r"], filename)
        if not hits is None:
            log(f"Found date hit from filename in {filename}: [{exp['r']}] => {hits[0]}")
            try:
                new_date = datetime.strptime(hits[0], exp['f'])
                if new_date > base_datetime:
                    working_datetime = new_date
                    break
            except:
                log(f"  ! Failed to convert into date format")
                # Ignore conversion failures
                pass

    result = base_datetime.timestamp()

    try:
        result = int(working_datetime.timestamp())
    except:
        # Return the base datetime if we fail to pull the timestamp from the file
        pass

    return result


def get_image_date_timestamp_for_image_path(img_path):
    global image_misses

    # First try to get the Google metadata for the image, if available
    google_metadata = try_get_google_metadata(img_path)
    if len(google_metadata) > 0:
        timestamp = int(google_metadata["creationTime"]["timestamp"])
        if timestamp > 0:
            return timestamp

    else:
        log(f"No Google metadata available for: {img_path}")

    # Next, try to read the modified date from the image exif, if available
    exif_metadata = {}
    try:
        exif_metadata = pyexiv2.Image(img_path).read_exif()
    except:
        # Any failure just consider this as no metadata exists
        pass

    if len(exif_metadata) > 0:
        for k in exif_metadata_keys_to_consider:
            if k in exif_metadata:
                datetime_object = datetime.strptime(exif_metadata[k], exif_metadata_datetime_format)
                return int(datetime_object.timestamp())
    else:
        log(f"No exif metadata available for: {img_path}")

    # Next, try to get some date information about the image from the filepath, if possible
    image_path_create_date = try_get_date_information_from_path(img_path)

    if image_path_create_date > 31564800:
        return int(image_path_create_date)
    else:
        log(f"Could not determine image date from image path: {img_path}")

    # Next, try to see if the modified date on image asset is less than its created date, likely accurate
    ctime = os.path.getctime(img_path)
    mtime = os.path.getmtime(img_path)

    if mtime < ctime:
        return int(mtime)

    # Finally, just set it to the start of 2024 if we can't get anything useful
    image_misses.append(img_path)
    return 0


def setup_dest_dir(dest_dir):
    if not os.path.isdir(dest_dir):
        log("Dest dir does not exist, creating")
        os.mkdir(dest_dir)

    elif any(os.listdir(dest_dir)):
        log("WARNING: Dest dir has existing files")


def setup_dest_dir_working_subdir(dest_dir, subdir):
    dest_dir_subpath = f"{dest_dir}/{subdir}"
    if not os.path.isdir(dest_dir_subpath):
        os.mkdir(dest_dir_subpath)


def process_dir(source_dir, dest_dir):
    log(f"Processing source directory: {source_dir} saving into {dest_dir}")

    setup_dest_dir(dest_dir)

    num_files_processed = 0
    subdir_num = 0

    setup_dest_dir_working_subdir(dest_dir, subdir_num)

    for root, dir, files in os.walk(source_dir):
        for ex in files_to_consider:
            for file_name in fnmatch.filter(files, f"*.{ex}"):
                asset_path = f"{root}/{file_name}"
                asset_filename = os.path.basename(asset_path).split(".")[0].replace(" ", "-")
                timestamp = get_image_date_timestamp_for_image_path(asset_path)
                if timestamp == 0:
                    log(f"Using fallback date {fallback_image_date} start for {asset_path}")
                    timestamp = fallback_image_date.timestamp()
                new_asset_path = None
                itr = 0
                while new_asset_path is None:
                    new_asset_path = f"{dest_dir}/{subdir_num}/{asset_filename}.{timestamp}.{itr}{os.path.splitext(asset_path)[1]}"
                    if os.path.exists(new_asset_path):
                        itr += 1
                        new_asset_path = None
                log(f"new_asset_path: {new_asset_path}")
                shutil.copyfile(asset_path, new_asset_path)
                os.utime(new_asset_path, (timestamp, timestamp))

                # On Windows we need setctime to update the created time
                if os.name == "nt":
                    setctime(new_asset_path, timestamp)

                num_files_processed += 1
                if num_files_processed % max_output_dir_file_limit == 0:
                    subdir_num += 1
                    setup_dest_dir_working_subdir(dest_dir, subdir_num)

    for img_path in image_misses:
        log(f"Failed to parse date for image: {img_path}")

    return num_files_processed


def main(args):
    if len(args) < 3:
        print("Expected usage: python3.exe update.py <source_dir> <dest_dir>")
        exit()

    if args[1] == args[2]:
        print("Source dir and dest dir cannot be the same directory")
        exit()

    input_dir = args[1]
    output_dir = args[2]

    num_files = process_dir(input_dir, output_dir)

    tool_info = json.dumps({
        "total_files": num_files,
        "misses": image_misses
    })

    with open(f"{output_dir}/{output_info_file}", "w+") as f:
        f.write(tool_info)


if __name__ == "__main__":
    main(sys.argv)
