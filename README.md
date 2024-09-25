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