package geoautogroup

import (
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

func GetTimeIndex(paths []string) (ti *geoindex.TimeIndex, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    ti = geoindex.NewTimeIndex()
    gc := geoindex.NewGeographicCollector(ti, nil)

    err = geoindex.RegisterImageFileProcessors(gc)
    log.PanicIf(err)

    err = geoindex.RegisterDataFileProcessors(gc)
    log.PanicIf(err)

    for _, scanPath := range paths {
        err := gc.ReadFromPath(scanPath)
        log.PanicIf(err)
    }

    return ti, nil
}
