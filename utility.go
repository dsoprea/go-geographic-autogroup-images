package geoautogroup

import (
    "fmt"
    "time"

    "github.com/dsoprea/go-geographic-attractor/index"
    "github.com/dsoprea/go-geographic-attractor/parse"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
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

    ci = geoattractorindex.NewCityIndex()

    err = ci.Load(gp, f)
    log.PanicIf(err)

    return ci, nil
}

func GetTimeIndex(paths []string, imageTimestampSkew time.Duration) (ti *geoindex.TimeIndex, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    ti = geoindex.NewTimeIndex()
    gc := geoindex.NewGeographicCollector(ti, nil)

    err = geoindex.RegisterImageFileProcessors(gc, imageTimestampSkew)
    log.PanicIf(err)

    err = geoindex.RegisterDataFileProcessors(gc)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    return ti, nil
}

func GetCondensedDatetime(t time.Time) string {
    return fmt.Sprintf("%d%02d%02d-%02d%02d%02d", t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second())
}
