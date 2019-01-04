package geoautogroup

import (
    "path"
    "testing"

    "github.com/dsoprea/go-logging"
)

func TestGetCityIndex(t *testing.T) {
    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    ci, err := GetCityIndex(countriesFilepath, citiesFilepath)
    log.PanicIf(err)

    sydneyCoordinates := []float64{-33.86785, 151.20732}

    _, _, cr, err := ci.Nearest(sydneyCoordinates[0], sydneyCoordinates[1])
    log.PanicIf(err)

    if cr.Id != "2147714" {
        t.Fatalf("ID in result is not correct: [%s]", cr.Id)
    }
}
