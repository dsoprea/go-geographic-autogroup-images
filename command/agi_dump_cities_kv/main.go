package main

import (
    "fmt"
    "os"

    "github.com/dsoprea/go-logging"
    "github.com/jessevdk/go-flags"

    "github.com/akrylysov/pogreb"
    "github.com/dsoprea/go-geographic-attractor/index"

    // We need the types to be registered for gob-decoding.
    _ "github.com/dsoprea/go-geographic-autogroup-images"
)

type parameters struct {
    CityDatabaseFilepath string `long:"city-db-filepath" description:"File-path of city database. Will be created if does not exist. If not provided a temporary one is used." required:"true"`
}

var (
    arguments = new(parameters)
)

func main() {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)
            os.Exit(1)
        }
    }()

    p := flags.NewParser(arguments, flags.Default)

    _, err := p.Parse()
    if err != nil {
        os.Exit(1)
    }

    // Print count.

    kv, err := pogreb.Open(arguments.CityDatabaseFilepath, nil)
    log.PanicIf(err)

    fmt.Printf("Records: (%d)\n", kv.Count())
    fmt.Printf("\n")

    kv.Close()

    // Dump contents.

    ci := geoattractorindex.NewCityIndex(
        arguments.CityDatabaseFilepath,
        geoattractorindex.DefaultMinimumLevelForUrbanCenterAttraction,
        geoattractorindex.DefaultUrbanCenterMinimumPopulation)

    defer ci.Close()

    err = ci.KvDump()
    log.PanicIf(err)
}
