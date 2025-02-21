import subprocess
import sys
import json

# This should be run on the server machine
server_url = "http://localhost:2283"


def main(args):
    try:
        with open(args[1], "r") as f:
            tasks = json.load(f)
            if not tasks:
                print("Error: JSON file is empty")
                sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    if len(tasks) == 0:
        print("No tasks found in the file.")
        sys.exit(1)

    for task in tasks:
        key = task["key"]
        for job in task["jobs"]:
            album_id = job["album_id"]
            for face_id in job["faces"]:
                try:
                    result = subprocess.run([
                        'immich-face-to-album',
                        '--key', key,
                        '--server', server_url,
                        '--face', face_id,
                        '--album', album_id
                    ], capture_output=True, text=True, check=True)
                    print(result.stdout)
                except subprocess.CalledProcessError as e:
                    print(f"Error executing subprocess: {e}")
                    print(e.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python face-to-album.py <tasks_file>")
        sys.exit(1)

    main(sys.argv)
