package geoautogroup

import (
    "path"
    "testing"

    "github.com/dsoprea/go-logging"
)

func TestCityIndex(t *testing.T) {
    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")
    ci := CityIndex(countriesFilepath, citiesFilepath)

    sydneyCoordinates := []float64 { -33.86785, 151.20732 }

    _, _, cr, err := ci.Nearest(sydneyCoordinates[0], sydneyCoordinates[1])
    log.PanicIf(err)

    if cr.Id != "2147714" {
        t.Fatalf("ID in result is not correct: [%s]", cr.Id)
    }
}
