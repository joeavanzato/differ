# differ
### File System Snapshots

#### What is it?

differ is a tool for generating and comparing snapshots of logical drives for any necessary purpose - this may include tasks such as determining changes made by a specific piece of software, changes between Windows patches, malware analysis/sandboxing, integrity checks, etc.

#### Why?

differ was created because I had a need to perform a modular file system snapshot and could not identify a simple but suitable tool for this task.

#### How to use?

differ can be run both through command-line arguments or fed a configuration file - the easiest way to use it is to download the most recent build - this will include differ.exe and differ_config.json.

The included default configuration file is shown below and will perform a recursive scan across the entire C:\ logical drive, hashing each file along the way with no restrictions.

```json
{
    "directories": [
        "C:\\"
    ],
    "use_extension_allowlist": false,
    "extension_allowlist": [
        ".exe"
    ],
    "use_extension_blocklist": false,
    "extension_blocklist": [
        ".txt"
    ],
    "hash_enabled": true,
    "hash_algorithm": "sha1",
    "do_csv_export": true
}
```

* directories - specify a list of directories to walk recursively for snapshot generation
* use_extension_allowlist - if true, will skip all files that do not possess an extension present in the allowlist
* use_extension_blocklist - if true, will skip all files that have an extension present in the blocklist
* hash_enabled - if true, will hash all included files
* hash_algorithm - can be sha1/sha256/md5
* do_csv_export - if true, will generate a CSV output in addition to parquet

When differ completes, it will store a .parquet file in the current working directory that contains the timestamp and hostname of the snapshot.

#### Comparing Snapshots
To compare two separate snapshots, use the '-compare' argument as follows:
```
differ.exe -compare 1727205513801559400_DESKTOP-KH2I9H2_differ_snapshot.parquet,1727224094973553500_DESKTOP-KH2I9H2_differ_snapshot.parquet
```
differ will perform a few different checks when looking for changes:
* Files with the same path, name and extension but that...
  * Have different hashes (modification)
  * Have different modification times (modification)
  * Have different file sizes (modification)
* Files that do not appear in the older snapshot but do appear in the newer one (creation)
* Files that do not appear in the newer snapshot but do appear in the previous one (deletion)

All differences are written to a CSV output file (snapshot_diff.csv) in the current working directory.

