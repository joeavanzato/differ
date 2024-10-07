# differ
## File System Metadata Snapshots made Easy

### What is it?

differ is a purpose-built tool for generating and comparing ('diffing') metadata snapshots of logical drives for any necessary purpose - this may include tasks such as determining changes made by a specific piece of software, changes between Windows patches, malware analysis/sandboxing, integrity checks, etc.

### Why?

differ was created because I had a need to perform a configurable file system metadata snapshot and subsequent comparison and I could not identify a simple and flexible open-source tool for this task.

Example Usecases Include:
* Baselining the contents of a logical drive to identify all changes following a system/software change
* Establishing a baseline for use in Incident Response processes and to identify changes in system files or created/deleted files following a breach
* Identifying differences in pre- and post- metadata snapshots during dynamic malware analysis (files created, files modified, files deleted)
* Quickly hashing files in any number of directories based on extension allow or block lists to identify any unwanted software
* Feeding data into allow/block lists to further DFIR processes/investigations
* Hunting for specific file-types across a system or specific directories

### How to use?

differ can be run both through command-line arguments or fed a configuration file - the easiest way to use it is to download the most recent build - this will include differ.exe and differ_config.json.


### Configuration File

To launch differ using a configuration file, just tell it where to find it like below;
```
differ.exe -config "configs\full_system_snapshot.json"
differ.exe -config "configs\full_scan_common_malware_extensions.json"
differ.exe -config "some\\path\\to\\config.json"
```

The full_system_snapshot configuration file is shown below - this configuration tells differ to recursively snapshot the metadata for all files starting at C:\ with no restrictions on extensions and performing the SHA1 hash of each encountered file.  CSV export is disabled by default.

On a common personal system using a nearly-full 2 TB M.2 SSD, this type of scan will take approximately 15-30 minutes depending on CPU availability.  The type of disk drive and connection mechanism will greatly influence the speed of the snapshot due to the potential for increased read-times.  I would recommend only snapshotting required directories and extensions when possible.

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
    "do_csv_export": false
}
```

* directories - specify a list of directories to walk recursively for snapshot generation
* use_extension_allowlist - if true, will skip all files that do not possess an extension present in the allowlist
* use_extension_blocklist - if true, will skip all files that have an extension present in the blocklist
* hash_enabled - if true, will hash all included files
* hash_algorithm - can be sha1/sha256/md5
* do_csv_export - if true, will generate a CSV output in addition to parquet

By default, differ will store a *.parquet file in the current working directory that contains the UNIX timestamp and hostname of the snapshot, such as '1727226208164680600_DESKTOP-KH2I9H2_differ_snapshot'.

Enabling CSV exports results in an immediately human-readable file being produced if the user doesn't want to convert the provided parquet to some other format - this is mainly done for storage purposes.


### Command-Line Arguments

```
-config some_file.json : When specified, differ will ignore all other command-line arguments and rely solely on the data contained within the configuration file for execution.
-directory "C:\\" : Tells differ the directory to use as the starting point for a recursive file-walk snapshot
-csv : Tells differ to also produce CSV output in addition to the default Parquet
-hash md5 / -hash sha1 / -hash sha256 : Tells differ to also compute the hash of all scanned files using one of the specified algorithms
-compare file1,file2 : Tells differ to 'diff' the two provided files - differ will automatically attempt to determine which one is older/newer based on the file naming format
```

### Comparing Snapshots
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

Be aware there are caveats here - if a file is moved between two directories, we will count that as both a deletion and creation since we are not doing 'hash-scanning' across the entire snapshot at this time.


### Common Extension Lists
For convenience, a few configuration files are provided inside the configs directory for common use-cases.  They are detailed below;

* full_system_snapshot
  * Recursively snapshot an entire drive starting at C:\ with no restrictions on extension and also performing SHA1 hash.
* quick_common_malware_hashscan.json
  * Contains common directories where malware often lives and an extension allow-list for the most common file types encountered during incidents.
* full_scan_common_malware_extensions.json
  * Same as above but will scan for common malware extensions across the entire logical drive starting at C:\.

