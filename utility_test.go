package geoautogroup

import (
    "bytes"
    "path"
    "testing"
    "time"

    "github.com/dsoprea/go-geographic-attractor/index"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
)

func getTestCityIndex() (ci *geoattractorindex.CityIndex) {
    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    ci, err := GetCityIndex(countriesFilepath, citiesFilepath)
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
