package geoautogroup

import (
    "os"

    "github.com/dsoprea/go-logging"
    "github.com/dsoprea/go-geographic-attractor/parse"
    "github.com/dsoprea/go-geographic-attractor/index"
)

func CityIndex(countriesFilepath, citiesFilepath string) (ci *geoattractorindex.CityIndex) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    // Load countries.

    f, err := os.Open(countriesFilepath)
    log.PanicIf(err)

    defer f.Close()

    countries, err := geoattractorparse.BuildGeonamesCountryMapping(f)
    log.PanicIf(err)

    // Load cities.

    gp := geoattractorparse.NewGeonamesParser(countries)

    g, err := os.Open(citiesFilepath)
    log.PanicIf(err)

    defer g.Close()

    ci = geoattractorindex.NewCityIndex()

    err = ci.Load(gp, g)
    log.PanicIf(err)

    return ci
}
