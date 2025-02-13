Walks a Google image/video dump updating the modified date for images based on a few factors:
- Checks for Google's metadata dump for the asset first
- Secondly checks the asset's exiv2 metadata for a creation date
- Lastly checks the filename for date information

Required PIP packages:
- win32-setctime
- pyexiv2

Usage:
`python3.exe update.py <source_dir> <dest_dir>`