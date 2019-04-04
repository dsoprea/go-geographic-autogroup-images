package geoautogroup

import (
    "fmt"
    "io"
    "time"

    "encoding/csv"

    "github.com/dsoprea/go-geographic-attractor/index"
    "github.com/dsoprea/go-geographic-attractor/parse"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
)

const (
    GeographicSourceListfile = "Listfile"
)

func GetCityIndex(countriesFilepath, citiesFilepath string) (ci *geoattractorindex.CityIndex, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    gp, err := geoattractorparse.NewGeonamesParserWithFiles(countriesFilepath)
    log.PanicIf(err)

    f, err := geoattractorparse.GetCitydataReadCloser(citiesFilepath)
    log.PanicIf(err)

    defer f.Close()

    ci = geoattractorindex.NewCityIndex(minimumLevelForUrbanCenterAttraction, urbanCenterMinimumPopulation)

    err = ci.Load(gp, f, nil)
    log.PanicIf(err)

    return ci, nil
}

// GetImageTimeIndex load an index with images.
func GetImageTimeIndex(paths []string, imageTimestampSkew time.Duration, cameraModels []string) (ti *geoindex.TimeIndex, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    ti = geoindex.NewTimeIndex()
    gc := geoindex.NewGeographicCollector(ti, nil)

    err = geoindex.RegisterImageFileProcessors(gc, imageTimestampSkew, cameraModels)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    return ti, nil
}

// GetLocationTimeIndex load an index with locations.
func GetLocationTimeIndex(paths []string) (ti *geoindex.TimeIndex, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    ti = geoindex.NewTimeIndex()
    gc := geoindex.NewGeographicCollector(ti, nil)

    err = geoindex.RegisterDataFileProcessors(gc)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    return ti, nil
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
            log.Panicf("Could not find record from source [%s] with ID [%s].", sourceName, id)
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
