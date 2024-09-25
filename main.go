package main

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/djherbis/times"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/rs/zerolog"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var fileChannel = make(chan string)
var enrichedFileChannel = make(chan File)
var hashType = ""

type RunningJobs struct {
	JobCount int
	Mw       sync.RWMutex
}

func (job *RunningJobs) GetJobs() int {
	job.Mw.RLock()
	defer job.Mw.RUnlock()
	return job.JobCount
}
func (job *RunningJobs) AddJob() {
	job.Mw.Lock()
	defer job.Mw.Unlock()
	job.JobCount += 1
}
func (job *RunningJobs) SubJob() {
	job.Mw.Lock()
	defer job.Mw.Unlock()
	job.JobCount -= 1
}

// Main Goals
// Take a recursive snapshot of a file system to capture the current state, starting at a specific directory
// What should be captured in a snapshot?
// Path, FileName, FileExtension, FileSize, SHA256, CreationDate, ModificationDate, AccessDate
// Then, allow for snapshot comparisons to find differences
// Key Metrics: Efficiency/Speed and Ease of Use/Outputs

/*type File struct {
	Path      string `avro:"path"`
	Name      string `avro:"name"`
	Extension string `avro:"extension"`
	SizeBytes int64  `avro:"bytes"`
	SHA256    string `avro:"sha256"`
	Created   int    `avro:"created"`
	Modified  int    `avro:"modified"`
	Accessed  int    `avro:"accessed"`
}*/

/*type File struct {
	Path      string `parquet:"name=path, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name      string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Extension string `parquet:"name=extension, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SizeBytes int64  `parquet:"name=sizebytes, type=INT64"`
	SHA256    string `parquet:"name=sha256, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Created   int32  `parquet:"name=Created, type=INT32"`
	Modified  int32  `parquet:"name=modified, type=INT32"`
	Accessed  int32  `parquet:"name=accessed, type=INT32"`
}*/

type File struct {
	Key       string // Will be used in-memory only for hash-table lookup of specific file when we are comparing two snapshots
	Path      string `parquet:"path,zstd"`
	Name      string `parquet:"name,zstd"`
	Extension string `parquet:"extension,zstd"`
	SizeBytes int64  `parquet:"sizeBytes"`
	Hash      string `parquet:"sha256,zstd"`
	Created   int64  `parquet:"created"`
	Modified  int64  `parquet:"modified"`
	Accessed  int64  `parquet:"accessed"`
}

type FileChange struct {
	ChangeType   string
	Path         string
	Name         string
	Extension    string
	OldSizeBytes int64
	NewSizeBytes int64
	OldHash      string
	NewHash      string
	OldCreated   int64
	NewCreated   int64
	OldModified  int64
	NewModified  int64
	OldAccessed  int64
	NewAccessed  int64
}

func (f FileChange) GetHeaders() []string {
	return []string{"ChangeType", "Path", "Name", "Extension", "OldSize", "NewSize", "OldHash", "NewHash", "OldCreated", "NewCreated", "OldModified", "NewModified", "OldAccessed", "NewAccessed"}
}

func (f FileChange) StringSlice() []string {
	return []string{f.ChangeType, f.Path, f.Name, f.Extension, strconv.FormatInt(f.OldSizeBytes, 10), strconv.FormatInt(f.NewSizeBytes, 10), f.OldHash, f.NewHash, strconv.Itoa(int(f.OldCreated)), strconv.Itoa(int(f.NewCreated)), strconv.Itoa(int(f.OldModified)), strconv.Itoa(int(f.NewModified)), strconv.Itoa(int(f.OldAccessed)), strconv.Itoa(int(f.NewAccessed))}
}

type Config struct {
	Directories           []string `json:"directories"`
	UseExtensionAllowlist bool     `json:"use_extension_allowlist"`
	ExtensionAllowlist    []string `json:"extension_allowlist"`
	UseExtensionBlocklist bool     `json:"use_extension_blocklist"`
	ExtensionBlocklist    []string `json:"extension_blocklist"`
	HashEnabled           bool     `json:"hash_enabled"`
	HashAlgorithm         string   `json:"hash_algorithm"`
	DoCSVExport           bool     `json:"do_csv_export"`
}

func (f File) StringSlice() []string {
	return []string{f.Path, f.Name, f.Extension, strconv.FormatInt(f.SizeBytes, 10), f.Hash, strconv.Itoa(int(f.Created)), strconv.Itoa(int(f.Modified)), strconv.Itoa(int(f.Accessed))}
}

var config Config
var usingConfig = false

var logFileName = "differ.log"

func setupLogger() zerolog.Logger {
	logFileName := logFileName
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		_, err := fmt.Fprintf(os.Stderr, "Couldn't Initialize Log File: %s", err)
		if err != nil {
			panic(nil)
		}
		panic(err)
	}
	cw := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		FormatLevel: func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("[%s]", i))
		},
	}
	cw.NoColor = true
	mw := io.MultiWriter(cw, logFile)
	logger := zerolog.New(mw).Level(zerolog.TraceLevel)
	logger = logger.With().Timestamp().Logger()
	return logger
}

func parseArgs() (map[string]any, error) {
	compare := flag.String("compare", "", "Should be a comma-separated argument for the two files two snapshot files for comparison - Example:  -compare \"1727205513801559400_DESKTOP-AB3I9H2_differ_snapshot.parquet,1727205778188514600_DESKTOP-AB3I9H2_differ_snapshot.parquet\"")
	directory := flag.String("directory", "", "Specify a directory to snapshot from") // specify a directory to snapshot or compare
	docsv := flag.Bool("csv", false, "Produce a CSV output in addition to default AVRO OCF for human readability")
	hashtype := flag.String("hash", "", "Will add the specified hash into the output (md5, sha1, sha256)")
	configfile := flag.String("config", "", "Location of a configuration file - if specified, will ignore other command-line arguments to only use options from the config file.  Example: -config differ_config.json")

	flag.Parse()

	tmpHash := strings.ToLower(*hashtype)
	if tmpHash != "" && tmpHash != "md5" && tmpHash != "sha1" && tmpHash != "sha256" {
		err1 := fmt.Sprintf("Unsupported Hash Type: %s - supported types include sha1, sha256, md5", tmpHash)
		arguments := map[string]any{}
		return arguments, errors.New(err1)
	}

	arguments := map[string]any{
		"compare":   *compare,
		"directory": *directory,
		"csv":       *docsv,
		"hash":      *hashtype,
		"config":    *configfile,
	}

	return arguments, nil
}

func readConfig(configFile string) error {
	f, err := os.Open(configFile)
	if err != nil {
		return err
	}
	defer f.Close()
	bytes, readErr := io.ReadAll(f)
	if readErr != nil {
		return readErr
	}
	jsonErr := json.Unmarshal(bytes, &config)
	if jsonErr != nil {
		return jsonErr
	}
	return nil
}

func validateConfig(logger zerolog.Logger) error {
	// remove invalid directories
	// validate hashing algorithm
	if config.HashEnabled {
		hashType = strings.ToLower(config.HashAlgorithm)
		if hashType != "" && hashType != "md5" && hashType != "sha1" && hashType != "sha256" {
			err1 := fmt.Sprintf("Unsupported Hash Type: %s - supported types include sha1, sha256, md5", hashType)
			return errors.New(err1)
		}
	}
	newDirs := make([]string, 0)
	for _, v := range config.Directories {
		exists, _ := validateDirectoryExists(v)
		if exists {
			newDirs = append(newDirs, v)
		} else {
			logger.Error().Msgf("Invalid Directory in Config File: %s", v)
		}
	}
	config.Directories = newDirs
	if len(newDirs) == 0 {
		err1 := fmt.Sprintf("0 valid directories found in config file!")
		return errors.New(err1)
	}
	return nil
}

func main() {
	logger := setupLogger()
	logger.Info().Msg("differ")
	logger.Info().Msg("Starting Up...")
	args, err := parseArgs()
	if err != nil {
		logger.Error().Msgf(err.Error())
		return
	}
	doingCompare := false
	if args["compare"] != "" {
		doingCompare = true
	}

	if args["config"].(string) != "" && !doingCompare {
		usingConfig = true
		configErr := readConfig(args["config"].(string))
		if configErr != nil {
			logger.Error().Msgf("Error Reading Provided Configuration File: %s", args["config"].(string))
			logger.Error().Msgf(configErr.Error())
			return
		}
		validateErr := validateConfig(logger)
		if validateErr != nil {
			logger.Error().Msgf("Error Reading Provided Configuration File: %s", args["config"].(string))
			logger.Error().Msgf(validateErr.Error())
		}
	}

	if !usingConfig && !doingCompare {
		if args["hash"].(string) != "" {
			config.HashEnabled = true
			config.HashAlgorithm = args["hash"].(string)
		}
		if args["csv"].(bool) {
			config.DoCSVExport = true
		}
		if args["directory"].(string) == "" {
			logger.Error().Msg("Must specify a directory to snapshot from! (Example: -directory \"C:\\\"")
			return
		}
		dirExists, dirErr := validateDirectoryExists(args["directory"].(string))
		if !dirExists {
			logger.Error().Msgf("The specified directory (%v) does not exist or is otherwise inaccessible!", args["directory"].(string))
			logger.Error().Msg(dirErr.Error())
			return
		}
		config.Directories = []string{args["directory"].(string)}
	}

	// TODO - Remove this
	// Ideas to reduce space
	// Convert Paths -> PathID and store int instead of string
	// Convert Extension -> ExtensionID and store int instead of string
	//
	/*	schema, schemaerr := avro.Parse(`{
		"name": "file",
		"type": "record",
		"fields": [
		  {"name": "path", "type": "string"},
		  {"name": "name", "type": "string"},
		  {"name": "extension", "type": "string"},
		  {"name": "bytes", "type": "long"},
		  {"name": "sha256", "type": "string"},
		  {"name": "created", "type": "int"},
		  {"name": "modified", "type": "int"},
		  {"name": "accessed", "type": "int"}
		]
		}`)

		if schemaerr != nil {
			logger.Error().Msgf(schemaerr.Error())
			return
		}*/

	if doingCompare {
		// do a snapshot comparison between two files
		compareString := args["compare"].(string)
		// should be a comma-delimited string containing the paths of two files
		files := strings.SplitN(compareString, ",", 2)
		if len(files) != 2 {
			logger.Error().Msgf("Could not split compare argument into two strings - missing single comma?")
			return
		}
		compareError := handleComparison(logger, files)
		if compareError != nil {
			logger.Error().Msgf(compareError.Error())
		}
		return
	}

	logger.Info().Msgf("Snapshot Targets: %s", config.Directories)
	logger.Info().Msgf("Hashing Enabled: %v", config.HashEnabled)
	if config.HashEnabled {
		logger.Info().Msgf("Hashing Algorithm: %v", config.HashAlgorithm)
	}
	logger.Info().Msgf("Extension Allowlist Enabled: %v", config.UseExtensionAllowlist)
	if config.UseExtensionAllowlist {
		logger.Info().Msgf("Extension Allowlist: %v", config.ExtensionAllowlist)
	}
	logger.Info().Msgf("Extension Blocklist Enabled: %v", config.UseExtensionBlocklist)
	if config.UseExtensionBlocklist {
		logger.Info().Msgf("Extension Blocklist: %v", config.ExtensionBlocklist)
	}
	logger.Info().Msgf("CSV Export Enabled: %v", config.DoCSVExport)
	snapErr := generateSnapshot(logger, args)
	if snapErr != nil {
		logger.Error().Msg(snapErr.Error())
	}
}

func handleComparison(logger zerolog.Logger, files []string) error {
	config.HashAlgorithm = "md5"
	oldSnap := ""
	newSnap := ""
	f1Timestamp := strings.SplitN(filepath.Base(files[0]), "_", 2)[0]
	f2Timestamp := strings.SplitN(filepath.Base(files[1]), "_", 2)[0]
	f1TimestampInt, f1err := strconv.Atoi(f1Timestamp)
	if f1err != nil {
		errorText := fmt.Sprintf("Could not convert snapshot timestamp to integer: %v", files[0])
		return errors.New(errorText)
	}
	f2TimestampInt, f2err := strconv.Atoi(f2Timestamp)
	if f2err != nil {
		errorText := fmt.Sprintf("Could not convert snapshot timestamp to integer: %v", files[1])
		return errors.New(errorText)
	}
	if f1TimestampInt < f2TimestampInt {
		oldSnap = files[0]
		newSnap = files[1]
	} else {
		oldSnap = files[1]
		newSnap = files[0]
	}

	// Types of snapshot changes
	// Files can be deleted, modified, accessed or created
	// To detect changes, we will first load both datasets into memory
	// 1. We will first find check all cross-hashes - files that have the same hash in the same directory
	//  	-If the name is different, we will record this as a modification
	//		-If the name is the same but Access time is different, we will record this as an access
	//		-If everything is the same, will not record an update
	// This will remove a significant amount of data.
	// 2. We will then check for deleted files - this basically means checking if same file name disappears from each directory
	// 3. Finally we will check for new files - files that only appear in the newer dataset.
	PrintMemUsage()
	rowsOLD, f1rerr := parquet.ReadFile[File](oldSnap)
	if f1rerr != nil {
		errorText := fmt.Sprintf("Could not read snapshot file: %v", oldSnap)
		return errors.New(errorText)
	}
	oldHashSet := map[string]File{}
	for _, f := range rowsOLD {
		//oldHashSet[quickHash(fmt.Sprintf("%v%v%v%v%v%v%v%v", f.Path, f.Name, f.Extension, f.SizeBytes, f.Hash, f.Created, f.Accessed, f.Modified))] = f
		oldHashSet[quickHash(fmt.Sprintf("%v%v%v", f.Path, f.Name, f.Extension))] = f
	}
	rowsNEW, f2rerr := parquet.ReadFile[File](newSnap)
	if f2rerr != nil {
		errorText := fmt.Sprintf("Could not read snapshot file: %v", newSnap)
		return errors.New(errorText)
	}
	newHashSet := map[string]File{}
	for _, f := range rowsNEW {
		//newHashSet[quickHash(fmt.Sprintf("%v%v%v%v%v%v%v%v", f.Path, f.Name, f.Extension, f.SizeBytes, f.Hash, f.Created, f.Accessed, f.Modified))] = f
		newHashSet[quickHash(fmt.Sprintf("%v%v%v", f.Path, f.Name, f.Extension))] = f
	}
	fmt.Println(len(newHashSet))

	// CSV STUFF
	var changeRecordChannel = make(chan FileChange)
	var csvWG sync.WaitGroup
	csvOut := fmt.Sprintf("snapshot_diff.csv")
	csvOutFile, csverr := os.Create(csvOut)
	if csverr != nil {
		return csverr
	}
	headers := FileChange{}.GetHeaders()
	writer := csv.NewWriter(csvOutFile)
	werr := writer.Write(headers)
	if werr != nil {
		logger.Error().Msg(werr.Error())
		return werr
	}
	csvWG.Add(1)
	go csvChangeWriteListener(changeRecordChannel, writer, logger, csvOutFile, 10000, &csvWG)
	///

	// First remove duplicates - these are files that are an exact match and as such do not need further inspection
	for k, newFile := range newHashSet {
		changeRecord := FileChange{
			ChangeType:   "",
			Path:         newFile.Path,
			Name:         newFile.Name,
			Extension:    newFile.Extension,
			OldSizeBytes: 0,
			NewSizeBytes: newFile.SizeBytes,
			OldHash:      "",
			NewHash:      newFile.Hash,
			OldCreated:   0,
			NewCreated:   newFile.Created,
			OldModified:  0,
			NewModified:  newFile.Modified,
			OldAccessed:  0,
			NewAccessed:  newFile.Accessed,
		}
		oldFile, ok := oldHashSet[k]
		if ok {
			/*			// remove from new and old hash sets since there is an identical match which indicates 0 file change
						delete(oldHashSet, k)
						delete(newHashSet, k)*/
			// file matches path/name/extension and hash
			changeRecord.OldHash = oldFile.Hash
			changeRecord.OldCreated = oldFile.Created
			changeRecord.OldAccessed = oldFile.Accessed
			changeRecord.OldModified = oldFile.Modified
			changeRecord.OldSizeBytes = oldFile.SizeBytes
			if oldFile.Hash != newFile.Hash {
				// Modification Changed
				delete(oldHashSet, k)
				delete(newHashSet, k)
				changeRecord.ChangeType = "Modified"
				// Removing 'Access' comparison for now as it's not very valid since we are accessing it just to grab size/hash
			} else if oldFile.SizeBytes != newFile.SizeBytes {
				// Size of File Changed
				delete(oldHashSet, k)
				delete(newHashSet, k)
				changeRecord.ChangeType = "Modified"
			} else {
				// Identical Files
				delete(oldHashSet, k)
				delete(newHashSet, k)
				changeRecord.ChangeType = "REMOVE"
			}
		} else {
			// File Creation
			delete(oldHashSet, k)
			delete(newHashSet, k)
			changeRecord.ChangeType = "Created"
		}
		if changeRecord.ChangeType != "REMOVE" {
			changeRecordChannel <- changeRecord
		}
	}
	// everything remaining in the old hashset is a 'deleted' file
	for _, oldFile := range oldHashSet {
		changeRecord := FileChange{
			ChangeType:   "Deleted",
			Path:         oldFile.Path,
			Name:         oldFile.Name,
			Extension:    oldFile.Extension,
			OldSizeBytes: oldFile.SizeBytes,
			NewSizeBytes: 0,
			OldHash:      oldFile.Hash,
			NewHash:      "",
			OldCreated:   oldFile.Created,
			NewCreated:   0,
			OldModified:  oldFile.Modified,
			NewModified:  0,
			OldAccessed:  oldFile.Accessed,
			NewAccessed:  0,
		}
		changeRecordChannel <- changeRecord
	}
	close(changeRecordChannel)
	csvWG.Wait()
	fmt.Println(len(newHashSet))

	PrintMemUsage()

	return nil
}

func quickHash(s string) string {
	hash := md5.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

func PrintMemUsage() {
	// Courtesy of https://gist.github.com/j33ty/79e8b736141be19687f565ea4c6f4226
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	// Courtesy of https://gist.github.com/j33ty/79e8b736141be19687f565ea4c6f4226
	return b / 1024 / 1024
}

func csvChangeWriteListener(c chan FileChange, w *csv.Writer, logger zerolog.Logger, outputF *os.File, bufferSize int, wait *sync.WaitGroup) {
	defer outputF.Close()
	defer wait.Done()
	tempRecords := make([][]string, 0)
	for {
		record, ok := <-c
		if !ok {
			break
		} else if len(tempRecords) <= bufferSize {
			tempRecords = append(tempRecords, record.StringSlice())
		} else {
			err := w.WriteAll(tempRecords)
			if err != nil {
				logger.Error().Msg(err.Error())
			}
			tempRecords = nil
		}
	}
	err := w.WriteAll(tempRecords)
	if err != nil {
		logger.Error().Msg(err.Error())
	}
	w.Flush()
	err = w.Error()
	if err != nil {
		logger.Error().Msg(err.Error())
	}
}

func csvWriteListener(c chan File, w *csv.Writer, logger zerolog.Logger, outputF *os.File, bufferSize int, wait *sync.WaitGroup) {
	defer outputF.Close()
	defer wait.Done()
	tempRecords := make([][]string, 0)
	for {
		record, ok := <-c
		if !ok {
			break
		} else if len(tempRecords) <= bufferSize {
			tempRecords = append(tempRecords, record.StringSlice())
		} else {
			err := w.WriteAll(tempRecords)
			if err != nil {
				logger.Error().Msg(err.Error())
			}
			tempRecords = nil
		}
	}
	err := w.WriteAll(tempRecords)
	if err != nil {
		logger.Error().Msg(err.Error())
	}
	w.Flush()
	err = w.Error()
	if err != nil {
		logger.Error().Msg(err.Error())
	}
}

// Checks if the provided path exists on the filesystem and is a directory
func validateDirectoryExists(dir string) (bool, error) {
	d, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return false, err
	}
	if d.IsDir() {
		return true, nil
	}
	return false, err
}

func processEnrichedFiles(c chan File, wg *sync.WaitGroup, timestamp string) {
	defer wg.Done()
	recordBuffer := 100000
	tempRecords := make([]File, 0)
	//tempBytes := make([][]byte, 0)

	// TODO - rework this because this is poor logic for quitting
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Fatal Error Acquiring Hostname!")
		return
	}
	outputFileName := fmt.Sprintf("%s_%v_differ_snapshot.parquet", timestamp, hostname)
	f, ferr := os.Create(outputFileName)
	if ferr != nil {
		fmt.Println(ferr)
		return
	}
	defer f.Close()

	// working for parquet-go
	// timings on a user documents dir for compression method when assigning the same method to all struct fields
	/*	none - 5:59 - 723 MB - 10k
		snappy - 4:56 - 296 MB- 10k
		lz4raw - 5:07 - 723 MB- 10k
		zstd - 5:04 - 223 MB- 10k
		zstd - 5:02 - 222 MB - 100K
		gzip - 5:25 - 186 MB- 10k
		brotli - 5:42 - 200 MB- 10k
		lzo - 5:50 - 723 MB- 10k
	*/
	writer := parquet.NewWriter(f, parquet.Compression(&zstd.Codec{Level: zstd.SpeedFastest, Concurrency: 2}))
	//writer := parquet.NewWriter(f)
	defer writer.Close()
	for {
		record, ok := <-c
		if !ok {
			break
		} else if len(tempRecords) < recordBuffer {
			if config.DoCSVExport {
				csvRecordChannel <- record
			}
			tempRecords = append(tempRecords, record)
		} else {
			for _, row := range tempRecords {
				if err := writer.Write(row); err != nil {
					fmt.Println(err)
				}
			}
			tempRecords = nil
		}
	}
	for _, row := range tempRecords {
		if err := writer.Write(row); err != nil {
			fmt.Println(err)
		}
	}

	//working for hamba/avro method
	/*	enc, err := ocf.NewEncoder(schema.String(), f)
		if err != nil {
			fmt.Println(err)
			return
		}

		ocf.WithCompressionLevel(5)
		for {
			record, ok := <-c
			if !ok {
				break
			} else {
				if doCSV {
					csvRecordChannel <- record
				}
				enc.Encode(record)
			}
		}
		enc.Flush()
		f.Sync()*/

	/*	for {
			record, ok := <-c
			if !ok {
				break
			} else if tempRecords < avroBuffer {
				data, averr := avro.Marshal(schema, record)
				if averr == nil {
					tempBytes = append(tempBytes, data)
				}
				tempRecords += 1
			} else {
				// flush to disk
				for _, v := range tempBytes {
					f.Write(v)
				}
				tempRecords = 0
				tempBytes = nil
			}
		}
		// In case there are any left
		for _, v := range tempBytes {
			f.Write(v)
		}*/

}

func getNewHasher() hash.Hash {
	if config.HashAlgorithm == "sha1" {
		return sha1.New()
	} else if config.HashAlgorithm == "sha256" {
		return sha256.New()
	} else if config.HashAlgorithm == "md5" {
		return md5.New()
	} else {
		return nil
	}
}

func processPaths(files []string, wg *sync.WaitGroup, r *RunningJobs) {
	defer wg.Done()
	defer r.SubJob()
	for _, v := range files {
		path, fileName := filepath.Split(v)
		//fileName := filepath.Base(v)
		fileExtension := filepath.Ext(fileName)

		if config.UseExtensionAllowlist && !slices.Contains(config.ExtensionAllowlist, fileExtension) {
			// extension not found in current allow-list
			continue
		}
		if config.UseExtensionBlocklist && slices.Contains(config.ExtensionBlocklist, fileExtension) {
			// extension found in current block-list
			continue
		}

		// read the file times
		filetimes, err := times.Stat(v)
		if err != nil {
			// todo
			continue
		}

		// adds ~25% time to do an os.Stat
		// get file size - potentially causes access update
		size := int64(0)
		fileinfo, err := os.Stat(v)
		if err != nil {
			// todo
		} else {
			size = fileinfo.Size()
		}

		// hash the file - modifies access time
		fileHash := ""
		if config.HashEnabled {
			f, openerr := os.Open(v)
			if openerr == nil {
				h := getNewHasher()
				_, copyerr := io.Copy(h, f)
				if copyerr == nil {
					fileHash = fmt.Sprintf("%x", h.Sum(nil))
					f.Close()
				}
				f.Close()
			}
			f.Close()
		}

		tmp := File{
			Path:      path,
			Name:      fileName,
			Extension: fileExtension,
			SizeBytes: size,
			Hash:      fileHash,
			Created:   0,
			Modified:  0,
			Accessed:  filetimes.AccessTime().Unix(),
		}
		if filetimes.HasChangeTime() {
			tmp.Modified = filetimes.ChangeTime().Unix()
		}
		if filetimes.HasBirthTime() {
			tmp.Created = filetimes.BirthTime().Unix()
		}
		enrichedFileChannel <- tmp
	}
}

func fileListener(c chan string, wg *sync.WaitGroup) {
	tempRecords := make([]string, 0)
	var fileWG sync.WaitGroup
	maxWorkers := 100
	maxRecordsPerWorker := 1000
	jobTracker := RunningJobs{
		JobCount: 0,
		Mw:       sync.RWMutex{},
	}
	for {
		record, ok := <-c
		if !ok {
			break
		} else if len(tempRecords) <= maxRecordsPerWorker {
			tempRecords = append(tempRecords, record)
		} else {
			if jobTracker.GetJobs() < maxWorkers {
				fileWG.Add(1)
				jobTracker.AddJob()
				go processPaths(tempRecords, &fileWG, &jobTracker)
				tempRecords = nil
			} else {
				for true {
					if jobTracker.GetJobs() < maxWorkers {
						fileWG.Add(1)
						jobTracker.AddJob()
						go processPaths(tempRecords, &fileWG, &jobTracker)
						tempRecords = nil
						break
					}
				}
			}
		}
	}
	if len(tempRecords) != 0 {
		fileWG.Add(1)
		jobTracker.AddJob()
		go processPaths(tempRecords, &fileWG, &jobTracker)
		tempRecords = nil
	}
	fileWG.Wait()
	close(enrichedFileChannel)
}

// Serves as the WalkDir function for each path visited - used to send files into the relevant chan for downstream consumption
func visit(path string, di fs.DirEntry, err error) error {
	//fmt.Printf("Visited: %s\n", path)
	// Send to cache channel - multiple goroutines consume on the other side, format structs and push to CSV/AVRO channels as necessary
	// Skip Directories
	if di.IsDir() {
		return nil
	}
	fileChannel <- path
	return nil
}

func walkDirectory(dir string, wg *sync.WaitGroup) {
	defer wg.Done()
	filepath.WalkDir(dir, visit)
}

var csvRecordChannel = make(chan File)

func generateSnapshot(logger zerolog.Logger, args map[string]any) error {
	start := time.Now()
	timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)

	var csvWG sync.WaitGroup
	if config.DoCSVExport {
		csvOut := fmt.Sprintf("%s_differ.csv", timestamp)
		csvOutFile, csverr := os.Create(csvOut)
		if csverr != nil {
			return csverr
		}
		headers := []string{"Path", "Name", "Extension", "Bytes", "Hash", "Created", "Changed", "Accessed"}
		writer := csv.NewWriter(csvOutFile)
		werr := writer.Write(headers)
		if werr != nil {
			logger.Error().Msg(werr.Error())
			return werr
		}
		csvWG.Add(1)
		go csvWriteListener(csvRecordChannel, writer, logger, csvOutFile, 10000, &csvWG)
	}

	var fileReaderWaitGroup sync.WaitGroup
	var enrichedFileProcessWaitGroup sync.WaitGroup
	enrichedFileProcessWaitGroup.Add(1)

	go fileListener(fileChannel, &fileReaderWaitGroup)
	go processEnrichedFiles(enrichedFileChannel, &enrichedFileProcessWaitGroup, timestamp)
	var fileWalkingWaitGroup sync.WaitGroup
	for _, v := range config.Directories {
		fileWalkingWaitGroup.Add(1)
		go walkDirectory(v, &fileWalkingWaitGroup)
	}
	fileWalkingWaitGroup.Wait()
	fileReaderWaitGroup.Wait()
	close(fileChannel)
	enrichedFileProcessWaitGroup.Wait()
	close(csvRecordChannel)
	csvWG.Wait()
	duration := time.Since(start)
	logger.Info().Msgf("Done Processing All Paths, Duration: %s", duration)

	return nil
}
