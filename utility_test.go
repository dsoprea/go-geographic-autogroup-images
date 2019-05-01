package geoautogroup

import (
    "bytes"
    "path"
    "testing"
    "time"

    "io/ioutil"

    "github.com/dsoprea/go-geographic-attractor/index"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
)

func getTestCityIndex() (ci *geoattractorindex.CityIndex) {
    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    ci, err := GetCityIndex("", countriesFilepath, citiesFilepath)
    log.PanicIf(err)

    return ci
}

func TestGetCityIndex(t *testing.T) {
    ci := getTestCityIndex()

    sydneyCoordinates := []float64{-33.86785, 151.20732}

    _, _, cr, err := ci.Nearest(sydneyCoordinates[0], sydneyCoordinates[1], false)
    log.PanicIf(err)

    if cr.Id != "2147714" {
        t.Fatalf("ID in result is not correct: [%s]", cr.Id)
    }
}

func TestLoadLocationListFile(t *testing.T) {
    ci := getTestCityIndex()

    s := `
GeoNames,2935022,2019-01-01T00:00:00Z
GeoNames,4887398,2019-02-10T05:00:00-05:00
`

    b := bytes.NewBufferString(s)
    ti := geoindex.NewTimeIndex()

    recordsCount, err := LoadLocationListFile(ci, "testfile", b, ti)
    log.PanicIf(err)

    if recordsCount != 2 {
        t.Fatalf("Expected exactly two records to be read from list-file: (%d)", recordsCount)
    }

    ts := ti.Series()
    if len(ts) != 2 {
        t.Fatalf("Expected exactly two records to be in the time-index: (%d)", len(ts))
    }

    record0 := ts[0]

    if record0.Time.Format(time.RFC3339) != "2019-01-01T00:00:00Z" {
        t.Fatalf("Record 1 timestamp not correct: [%v]", record0.Time)
    }

    gr0 := record0.Items[0].(*geoindex.GeographicRecord)

    if gr0.S2CellId != 5118850495264135201 {
        t.Fatalf("Record 1 cell not correct: (%d)", gr0.S2CellId)
    }

    record1 := ts[1]

    if record1.Time.Format(time.RFC3339) != "2019-02-10T05:00:00-05:00" {
        t.Fatalf("Record 2 timestamp not correct: [%v]", record1.Time)
    }

    gr1 := record1.Items[0].(*geoindex.GeographicRecord)

    if gr1.S2CellId != 9803822164217287575 {
        t.Fatalf("Record 2 cell not correct: (%d)", gr1.S2CellId)
    }
}

func TestGetImageTimeIndex(t *testing.T) {
    paths := []string{
        path.Join(testAssetsPath, "test_sources_path1"),
    }

    ti, err := GetImageTimeIndex(paths, time.Duration(0), nil, false)
    log.PanicIf(err)

    ts := ti.Series()

    if len(ts) != 4 {
        t.Fatalf("The number of files read is not correct: (%d)", len(ts))
    }

    first := ts[0].Time.Unix()
    last := ts[len(ts)-1].Time.Unix()

    if first != 1334871116 {
        t.Fatalf("First timestamp is not correct: (%d)", first)
    } else if last != 1528708357 {
        t.Fatalf("Last timestamp is not correct: (%d)", last)
    }
}

func TestGetLocationTimeIndex_JustDataSources_Create(t *testing.T) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)

            t.Fatalf("Test failed.")
        }
    }()

    paths := []string{
        path.Join(testAssetsPath, "test_sources_path1"),
    }

    f, err := ioutil.TempFile("", "")
    log.PanicIf(err)

    defer f.Close()

    filepath := f.Name()

    ti, dbAlreadyExists, dbUpdated, err := GetLocationTimeIndex(paths, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == true {
        t.Fatalf("DB is supposed to not already exist.")
    } else if dbUpdated == false {
        t.Fatalf("DB is supposed to have changed.")
    }

    ts := ti.Series()

    if len(ts) != 254 {
        t.Fatalf("The record count is not correct: (%d)", len(ts))
    }

    first := ts[0].Time.Unix()
    last := ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct.")
    } else if last != 1549024530 {
        t.Fatalf("Last timestamp not correct.")
    }

    // Do a read *without* the sources, now.

    ti, dbAlreadyExists, dbUpdated, err = GetLocationTimeIndex(nil, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == false {
        t.Fatalf("DB is supposed to already exist.")
    } else if dbUpdated == true {
        t.Fatalf("DB is supposed to not have changed.")
    }

    ts = ti.Series()

    if len(ts) != 254 {
        t.Fatalf("The record count is not correct: (%d)", len(ts))
    }

    first = ts[0].Time.Unix()
    last = ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct.")
    } else if last != 1549024530 {
        t.Fatalf("Last timestamp not correct.")
    }
}

func TestGetLocationTimeIndex_JustDataSources_Update_NoChange(t *testing.T) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)

            t.Fatalf("Test failed.")
        }
    }()

    paths := []string{
        path.Join(testAssetsPath, "test_sources_path1"),
    }

    f, err := ioutil.TempFile("", "")
    log.PanicIf(err)

    defer f.Close()

    filepath := f.Name()

    ti, dbAlreadyExists, dbUpdated, err := GetLocationTimeIndex(paths, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == true {
        t.Fatalf("DB is supposed to not already exist.")
    } else if dbUpdated == false {
        t.Fatalf("DB is supposed to have changed.")
    }

    ts := ti.Series()

    if len(ts) != 254 {
        t.Fatalf("The record count is not correct: (%d)", len(ts))
    }

    first := ts[0].Time.Unix()
    last := ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct.")
    } else if last != 1549024530 {
        t.Fatalf("Last timestamp not correct.")
    }

    // Reload and make sure it looks like it short-circuited (because there are
    // no changes) but returns the same data.

    ti, dbAlreadyExists, dbUpdated, err = GetLocationTimeIndex(paths, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == false {
        t.Fatalf("DB is supposed to already exist.")
    } else if dbUpdated == true {
        t.Fatalf("DB is supposed to have not changed.")
    }

    first = ts[0].Time.Unix()
    last = ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct.")
    } else if last != 1549024530 {
        t.Fatalf("Last timestamp not correct.")
    }

    // Do a read *without* the sources, now.

    ti, dbAlreadyExists, dbUpdated, err = GetLocationTimeIndex(nil, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == false {
        t.Fatalf("DB is supposed to already exist.")
    } else if dbUpdated == true {
        t.Fatalf("DB is supposed to not have changed.")
    }

    ts = ti.Series()

    if len(ts) != 254 {
        t.Fatalf("The record count is not correct: (%d)", len(ts))
    }

    first = ts[0].Time.Unix()
    last = ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct.")
    } else if last != 1549024530 {
        t.Fatalf("Last timestamp not correct.")
    }
}

func TestGetLocationTimeIndex_JustDataSources_Update_WithChange(t *testing.T) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)

            t.Fatalf("Test failed.")
        }
    }()

    paths := []string{
        path.Join(testAssetsPath, "test_sources_path1"),
    }

    f, err := ioutil.TempFile("", "")
    log.PanicIf(err)

    defer f.Close()

    filepath := f.Name()

    ti, dbAlreadyExists, dbUpdated, err := GetLocationTimeIndex(paths, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == true {
        t.Fatalf("DB is supposed to not already exist.")
    } else if dbUpdated == false {
        t.Fatalf("DB is supposed to have changed.")
    }

    ts := ti.Series()

    if len(ts) != 254 {
        t.Fatalf("The record count is not correct: (%d)", len(ts))
    }

    first := ts[0].Time.Unix()
    last := ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct.")
    } else if last != 1549024530 {
        t.Fatalf("Last timestamp not correct.")
    }

    // Reload and make sure it looks like it short-circuited (because there are
    // no changes) but returns the same data.

    paths = []string{
        path.Join(testAssetsPath, "test_sources_path2"),
    }

    ti, dbAlreadyExists, dbUpdated, err = GetLocationTimeIndex(paths, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == false {
        t.Fatalf("DB is supposed to already exist.")
    } else if dbUpdated == false {
        t.Fatalf("DB is supposed to have been.")
    }

    ts = ti.Series()

    if len(ts) != 215 {
        t.Fatalf("The record count is not correct: (%d)", len(ts))
    }

    first = ts[0].Time.Unix()
    last = ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct: (%d)", first)
    } else if last != 1549024516 {
        t.Fatalf("Last timestamp not correct: (%d)", last)
    }

    // Do a read *without* the sources, now.

    ti, dbAlreadyExists, dbUpdated, err = GetLocationTimeIndex(nil, filepath, false)
    log.PanicIf(err)

    if dbAlreadyExists == false {
        t.Fatalf("DB is supposed to already exist.")
    } else if dbUpdated == true {
        t.Fatalf("DB is supposed to not have changed.")
    }

    ts = ti.Series()

    if len(ts) != 215 {
        t.Fatalf("The record count is not correct: (%d)", len(ts))
    }

    first = ts[0].Time.Unix()
    last = ts[len(ts)-1].Time.Unix()

    if first != 1549002148 {
        t.Fatalf("First timestamp not correct: (%d)", first)
    } else if last != 1549024516 {
        t.Fatalf("Last timestamp not correct: (%d)", last)
    }
}
