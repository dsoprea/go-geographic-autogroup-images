package geoautogroup

import (
    "bytes"
    "errors"
    "fmt"
    "io"
    "os"
    "path"
    "sort"
    "time"

    "crypto/sha1"
    "encoding/csv"
    "encoding/gob"

    "github.com/dsoprea/go-geographic-attractor/index"
    "github.com/dsoprea/go-geographic-attractor/parse"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
    "github.com/dsoprea/go-time-index"
    "github.com/dsoprea/time-to-go"
    "gopkg.in/cheggaaa/pb.v1"
)

var (
    utilityLogger = log.NewLogger("geoautogroup.utility")
)

const (
    GeographicSourceListfile = "Listfile"
)

var (
    ErrLocationTimeIndexChecksumFail = errors.New("location time-index checksum failure")
)

func GetCityIndex(cityKvFilepath, countriesFilepath, citiesFilepath string, countryFilter []string, beVerbose bool) (ci *geoattractorindex.CityIndex, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    kvParentPath := path.Dir(cityKvFilepath)

    f, err := os.Open(kvParentPath)
    if err != nil {
        if os.IsNotExist(err) == true {
            err := os.Mkdir(kvParentPath, 0755)
            log.PanicIf(err)
        } else {
            log.Panic(err)
        }
    } else {
        f.Close()
    }

    f, err = os.Open(cityKvFilepath)

    var alreadyExists bool
    if err == nil {
        f.Close()
        alreadyExists = true
    } else if os.IsNotExist(err) == false {
        log.PanicIf(err)
    }

    ci = geoattractorindex.NewCityIndex(cityKvFilepath, minimumLevelForUrbanCenterAttraction, urbanCenterMinimumPopulation)
    ci.SetVerbose(beVerbose)

    if alreadyExists == true {
        count, err := ci.KvCount()
        log.PanicIf(err)

        if count == 0 {
            alreadyExists = false
            utilityLogger.Warningf(nil, "Cities database exists but is empty.")
        }
    }

    if alreadyExists == false {
        gp, err := geoattractorparse.NewGeonamesParserWithFiles(countriesFilepath)
        log.PanicIf(err)

        // Get record count. This takes a few seconds but pales in comparison
        // to the amount of time it takes to populate the DB. So, better to have
        // a progress information on the latter.

        if beVerbose == true {
            fmt.Printf("Counting city records.\n")
        }

        g, err := geoattractorparse.GetCitydataReadCloser(citiesFilepath)
        if err != nil {
            g.Close()
            log.Panic(err)
        }

        recordsCount, err := gp.Parse(g, nil)
        log.PanicIf(err)

        g.Close()

        // Actually process the data.

        ci.SetTotalRecords(recordsCount)

        g, err = geoattractorparse.GetCitydataReadCloser(citiesFilepath)
        log.PanicIf(err)

        defer g.Close()

        err = ci.Load(gp, g, nil, countryFilter)
        log.PanicIf(err)
    }

    return ci, nil
}

// GetImageTimeIndex load an index with images.
func GetImageTimeIndex(paths []string, imageTimestampSkew time.Duration, cameraModels []string, beVerbose bool) (ti *geoindex.TimeIndex, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    imageFileCount, err := CountImageFiles(paths)
    log.PanicIf(err)

    ti = geoindex.NewTimeIndex()
    gc := geoindex.NewGeographicCollector(ti, nil)

    var imageBar *pb.ProgressBar
    if beVerbose == true {
        imageBar = pb.New(imageFileCount)
        imageBar.Prefix("Loading images ")
        imageBar.SetMaxWidth(100)
        imageBar.Start()
    }

    progressCb := func(filepath string) (err error) {
        if imageBar != nil {
            imageBar.Increment()
        }

        return nil
    }

    gc.SetFileProcessedCallback(progressCb)

    err = geoindex.RegisterImageFileProcessors(gc, imageTimestampSkew, nil)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    if imageBar != nil {
        imageBar.Finish()
    }

    return ti, nil
}

// GetLocationTimeIndex loads/recovers an index with all found locations.
func GetLocationTimeIndex(paths []string, locationsDatabaseFilepath string, beVerbose bool) (ti *geoindex.TimeIndex, dbAlreadyExists, dbUpdated bool, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    hasSources := paths != nil && len(paths) > 0
    hasDatabase := locationsDatabaseFilepath != ""

    var locationStream *os.File
    if hasDatabase {
        f, err := os.Open(locationsDatabaseFilepath)
        if err == nil {
            dbAlreadyExists = true
            f.Close()
        } else if os.IsNotExist(err) == false {
            log.Panic(err)
        }

        utilityLogger.Infof(nil, "Creating time-series database.")
        locationStream, err = os.OpenFile(locationsDatabaseFilepath, os.O_CREATE|os.O_RDWR, 0644)
        log.PanicIf(err)
    }

    // Make sure the location-file, if there is one, gets closed.
    defer func() {
        if locationStream != nil {
            locationStream.Close()
        }
    }()

    if dbAlreadyExists == false && hasSources == false {
        log.Panicf("either location data-paths or an existing location database must be given")
    }

    var streamReader *timetogo.StreamReader
    var streamIterator *timetogo.Iterator
    var streamSeriesDataSha1 []byte
    var existingSisi timetogo.StreamIndexedSequenceInfo

    // We were given a database and it already exists, read the state of the
    // data from it.
    if dbAlreadyExists == true {
        streamReader = timetogo.NewStreamReader(locationStream)

        var err error

        streamIterator, err = timetogo.NewIterator(streamReader)
        if err != nil {
            if log.Is(err, io.EOF) == true {
                dbAlreadyExists = false
            } else {
                utilityLogger.Errorf(nil, err, "There was an issue reading your location database: [%s]. If it is corrupted, please delete the existing one and provide your location data-sources to this command.", locationsDatabaseFilepath)
                fmt.Printf("There was an issue reading your location database [%s]: [%s]. If it is corrupted, please delete the existing one and provide your location data-sources to this command.\n", locationsDatabaseFilepath, err.Error())

                log.Panic(err)
            }
        } else {
            // NOTE(dustin): !! We don't currently have a plan for cutting our location time-series into separate pieces stored in the stream. For now, just store and retrieve the first.

            count_ := streamIterator.Count()
            if count_ < 1 {
                log.Panicf("location database does not represent at least one series: (%d)", count_)
            } else if count_ > 1 {
                utilityLogger.Warningf(nil, "More than one series is in the stream, which shouldn't be the case. Just taking the first.")
            }

            existingSisi = streamIterator.SeriesInfo(0)

            // If we have a database but no data files.

            ts := make(timeindex.TimeSlice, 0)
            gsodd := timetogo.NewGobSingleObjectDecoderDatasource(&ts)

            existingSf, _, checksumOk, err := streamReader.ReadSeriesWithIndexedInfo(existingSisi, gsodd)
            log.PanicIf(err)

            streamSeriesDataSha1 = existingSf.SourceSha1()

            if checksumOk != true {
                log.PanicIf(ErrLocationTimeIndexChecksumFail)
            }

            ti = geoindex.NewTimeIndexFromSlice(ts)

            // No data sources, so what we have is far as we can go.
            if hasSources == false {
                utilityLogger.Debugf(nil, "Database has been read and checked, and no data sources were given. Returning data.")
                return ti, dbAlreadyExists, false, nil
            }
        }
    }

    // If we get here, we have data-paths but not necessarily an existing
    // location database.

    // Generate SHA1 for current data if current data was given.
    filesSha1, err := GetSha1ForPaths(paths)
    log.PanicIf(err)

    if dbAlreadyExists == false {
        utilityLogger.Debugf(nil, "Data sources were given and match, and no database exists. Database will be created.")
    } else {
        if bytes.Compare(streamSeriesDataSha1, filesSha1) == 0 {
            // We have data-sources and a database, and they both match. Return
            // what we already have. The update process would be a no-op, but
            // in order for us to get to that step we'd have to load the
            // location-index, which could be very expensive.

            utilityLogger.Debugf(nil, "Database has been read and checked. Data sources were given and match. Returning data.")
            return ti, dbAlreadyExists, false, nil
        } else {
            utilityLogger.Debugf(nil, "Database has been read and checked. Data sources were given but do not match. Database will be updated.")
        }
    }

    // The data on the disk and the database *do not* match.

    // Load location-index from data sources.

    dataFileCount, err := CountDataFiles(paths)
    log.PanicIf(err)

    ti = geoindex.NewTimeIndex()
    gc := geoindex.NewGeographicCollector(ti, nil)

    var dataBar *pb.ProgressBar
    if beVerbose == true {
        dataBar = pb.New(dataFileCount)
        dataBar.Prefix("Loading location data ")
        dataBar.SetMaxWidth(100)
        dataBar.Start()
    }

    progressCb := func(filepath string) (err error) {
        if dataBar != nil {
            dataBar.Increment()
        }

        return nil
    }

    gc.SetFileProcessedCallback(progressCb)

    err = geoindex.RegisterDataFileProcessors(gc)
    log.PanicIf(err)

    for _, dataPath := range paths {
        err := gc.ReadFromPath(dataPath)
        log.PanicIf(err)
    }

    if dataBar != nil {
        dataBar.Finish()
    }

    // Create/update the series data.

    ts := ti.Series()

    sf := timetogo.NewSeriesFooter1(
        ts[0].Time,
        ts[len(ts)-1].Time,
        uint64(len(ts)),
        filesSha1)

    gsoed := timetogo.NewGobSingleObjectEncoderDatasource(ts)

    updater := timetogo.NewUpdater(locationStream, gsoed)
    updater.AddSeries(sf)

    totalSize, stats, err := updater.Write()
    log.PanicIf(err)

    if stats.Skips != 0 {
        log.Panicf("update operation reported skips but shouldn't have: (%d)", stats.Skips)
    } else if stats.Adds != 1 {
        log.Panicf("update operation did not report any adds")
    }

    if stats.Drops > 0 && dbAlreadyExists == false {
        log.Panicf("update operation stated that there was a drop but there was no existing DB")
    } else if stats.Drops == 0 && dbAlreadyExists == true {
        log.Panicf("update operation stated that there were no drops but there *was* an existing DB and at this stage it *must* be different than our data")
    }

    utilityLogger.Debugf(nil, "Update complete. Location database is (%d) bytes.", totalSize)

    return ti, dbAlreadyExists, true, nil
}

func GetSha1ForPaths(paths []string) (filesSha1 []byte, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    gc := geoindex.NewGeographicCollector(nil, nil)

    err = geoindex.RegisterDataFileProcessors(gc)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    files := gc.VisitedFilepaths()

    sortedFiles := sort.StringSlice(files)
    sortedFiles.Sort()

    h := sha1.New()
    for _, filepath := range sortedFiles {
        f, err := os.Open(filepath)
        log.PanicIf(err)

        _, err = io.Copy(h, f)
        log.PanicIf(err)

        f.Close()
    }

    filesSha1 = h.Sum(nil)
    return filesSha1, nil
}

func CountImageFiles(paths []string) (count int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    gc := geoindex.NewGeographicCollector(nil, nil)

    err = geoindex.RegisterImageFileProcessors(gc, 0, nil)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    return gc.VisitedCount(), nil
}

func CountDataFiles(paths []string) (count int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    gc := geoindex.NewGeographicCollector(nil, nil)

    err = geoindex.RegisterDataFileProcessors(gc)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    return gc.VisitedCount(), nil
}

// GetCondensedDatetime returns a timestamp string in whatever timezone the
// input value is.
func GetCondensedDatetime(t time.Time) string {
    return fmt.Sprintf("%d%02d%02d-%02d%02d%02d", t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second())
}

// LoadLocationListFile allows the user to provide a custom list of locations
// and timestamps. This can be used to patch buggy location data.
func LoadLocationListFile(ci *geoattractorindex.CityIndex, filepath string, r io.Reader, ti *geoindex.TimeIndex) (recordsCount int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    c := csv.NewReader(r)

    c.Comment = '#'
    c.FieldsPerRecord = 3

    for i := 0; ; i++ {
        record, err := c.Read()
        if err != nil {
            if err == io.EOF {
                break
            }

            log.Panic(err)
        }

        sourceName := record[0]
        id := record[1]
        timestampPhrase := record[2]

        timestamp, err := time.Parse(time.RFC3339, timestampPhrase)
        if err != nil {
            log.Panicf("Could not parse [%s]: %s", timestampPhrase, err)
        }

        cr, err := ci.GetById(sourceName, id)
        if err != nil {
            if err == geoattractorindex.ErrNotFound {
                log.Panicf("Could not find record from source [%s] with ID [%s].", sourceName, id)
            }

            log.Panic(err)
        }

        gr := geoindex.NewGeographicRecord(
            GeographicSourceListfile,
            filepath,
            timestamp,
            true,
            cr.Latitude,
            cr.Longitude,
            nil)

        err = ti.AddWithRecord(gr)
        log.PanicIf(err)

        recordsCount++
    }

    return recordsCount, nil
}

func init() {
    gob.Register(map[string]interface{}{})
}
