package geoautogroup

import (
    "fmt"
    "path"
    "reflect"
    "testing"
    "time"

    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
    "github.com/dsoprea/go-time-index"
)

const (
    oneDay = time.Hour * 24
)

var (
    epochUtc = time.Unix(0, 0).UTC()

    chicagoCoordinates = []float64{41.85003, -87.65005}
    detroitCoordinates = []float64{42.33143, -83.04575}
    nycCoordinates     = []float64{40.71427, -74.00597}
    sydneyCoordinates  = []float64{-33.86785, 151.20732}
    joCoordinates      = []float64{-26.20227, 28.04363}
    dresdenCoordinates = []float64{51.05089, 13.73832}
)

func TestFindGroups_AddUnassigned(t *testing.T) {
    // locationIndex is just a non-empty index. We won't use it, but it needs to
    // be present with at least one entry.
    locationIndex := geoindex.NewTimeIndex()

    gr := geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file1", epochUtc, true, 1.1, 10.1, nil)
    locationIndex.AddWithRecord(gr)

    locationTs := locationIndex.Series()
    fg := NewFindGroups(locationTs, nil, nil)

    gr = &geoindex.GeographicRecord{
        S2CellId: 123,
    }

    reason := "some reason"

    fg.addUnassigned(gr, reason)

    unassignedRecords := fg.UnassignedRecords()

    if len(unassignedRecords) != 1 {
        t.Fatalf("There wasn't exactly one unassigned record: (%d)", len(fg.unassignedRecords))
    }

    ur := unassignedRecords[0]

    if ur.Geographic.Equal(gr) == false {
        t.Fatalf("Geographic record not stored correctly.")
    } else if ur.Reason != reason {
        t.Fatalf("Reason not stored correctly.")
    }
}

func getTestLocationTs() timeindex.TimeSlice {
    timeBase := epochUtc

    locationTi := geoindex.NewTimeIndex()

    timeSeries := map[string]struct {
        timestamp time.Time
        latitude  float64
        longitude float64
    }{
        "file00.gpx": {timeBase.Add(time.Hour*0 + time.Minute*0), 1.1, 10.1},
        "file01.gpx": {timeBase.Add(time.Hour*0 + time.Minute*1), 1.2, 10.2},
        "file02.gpx": {timeBase.Add(time.Hour*0 + time.Minute*2), 1.3, 10.3},
        "file03.gpx": {timeBase.Add(time.Hour*0 + time.Minute*3), 1.4, 10.4},
        "file04.gpx": {timeBase.Add(time.Hour*0 + time.Minute*4), 1.5, 10.5},

        "file10.gpx": {timeBase.Add(time.Hour*1 + time.Minute*0), 2.1, 20.1},
        "file11.gpx": {timeBase.Add(time.Hour*1 + time.Minute*5), 2.2, 20.2},
        "file12.gpx": {timeBase.Add(time.Hour*1 + time.Minute*10), 2.3, 20.3},
        "file13.gpx": {timeBase.Add(time.Hour*1 + time.Minute*15), 2.4, 20.4},
        "file14.gpx": {timeBase.Add(time.Hour*1 + time.Minute*20), 2.5, 20.5},

        "file20.gpx": {timeBase.Add(time.Hour*2 + time.Minute*0), 3.1, 30.1},
        "file21.gpx": {timeBase.Add(time.Hour*2 + time.Minute*1), 3.2, 30.2},
        "file22.gpx": {timeBase.Add(time.Hour*2 + time.Minute*2), 3.3, 30.3},
        "file23.gpx": {timeBase.Add(time.Hour*2 + time.Minute*3), 3.4, 30.4},
        "file24.gpx": {timeBase.Add(time.Hour*2 + time.Minute*4), 3.5, 30.5},

        "file30.gpx": {timeBase.Add(time.Hour*3 + time.Minute*0), 4.1, 40.1},
        "file31.gpx": {timeBase.Add(time.Hour*3 + time.Minute*10), 4.2, 40.2},
        "file32.gpx": {timeBase.Add(time.Hour*3 + time.Minute*20), 4.3, 40.3},
        "file33.gpx": {timeBase.Add(time.Hour*3 + time.Minute*30), 4.4, 40.4},
        "file34.gpx": {timeBase.Add(time.Hour*3 + time.Minute*40), 4.5, 40.5},

        "file40.gpx": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*0), 5.1, 50.1},
        "file41.gpx": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*10), 5.2, 50.2},
        "file42.gpx": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*20), 5.3, 50.3},
        "file43.gpx": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*30), 5.4, 50.4},
        "file44.gpx": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*40), 5.5, 50.5},

        "file50.gpx": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*0), 6.1, 60.1},
        "file51.gpx": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*10), 6.2, 60.2},
        "file52.gpx": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*20), 6.3, 60.3},
        "file53.gpx": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*30), 6.4, 60.4},
        "file54.gpx": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*40), 6.5, 60.5},
    }

    for filepath, x := range timeSeries {
        gr := geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, filepath, x.timestamp, true, x.latitude, x.longitude, nil)
        locationTi.AddWithRecord(gr)
    }

    return locationTi.Series()
}

func TestFindGroups_FindLocationByTime_ExactMatch(t *testing.T) {
    locationTs := getTestLocationTs()

    fg := NewFindGroups(locationTs, nil, nil)

    imageTimestamp := epochUtc.Add(time.Hour*1 + time.Minute*10)

    imageTe := timeindex.TimeEntry{
        Time:  imageTimestamp,
        Items: nil,
    }

    matchedTe, err := fg.findLocationByTimeBestGuess(imageTe)
    log.PanicIf(err)

    expectedLocationTimestamp := epochUtc.Add(time.Hour*1 + time.Minute*10)

    if matchedTe.Time != expectedLocationTimestamp {
        t.Fatalf("The matched location timestamp is not correct: [%s] != [%s]", matchedTe.Time, expectedLocationTimestamp)
    } else if len(matchedTe.Items) != 1 {
        t.Fatalf("Expected exactly one location item to be matched: %v\n", matchedTe.Items)
    }

    gr := matchedTe.Items[0].(*geoindex.GeographicRecord)

    expectedLatitude := float64(2.3)
    if gr.Latitude != expectedLatitude {
        t.Fatalf("Matched latitude not correct: [%.10f] != [%.10f]", gr.Latitude, expectedLatitude)
    }

    expectedLongitude := float64(20.3)
    if gr.Longitude != expectedLongitude {
        t.Fatalf("Matched longitude not correct: [%.10f] != [%.10f]", gr.Longitude, expectedLongitude)
    }
}

func TestFindGroups_FindLocationByTime_JustBeforeLocationRecord(t *testing.T) {
    locationTs := getTestLocationTs()

    fg := NewFindGroups(locationTs, nil, nil)

    imageTimestamp := epochUtc.Add(time.Hour*1 + time.Minute*9)

    imageTe := timeindex.TimeEntry{
        Time:  imageTimestamp,
        Items: nil,
    }

    matchedTe, err := fg.findLocationByTimeBestGuess(imageTe)
    log.PanicIf(err)

    expectedLocationTimestamp := epochUtc.Add(time.Hour*1 + time.Minute*10)

    if matchedTe.Time != expectedLocationTimestamp {
        t.Fatalf("The matched location timestamp is not correct: [%s] != [%s]", matchedTe.Time, expectedLocationTimestamp)
    } else if len(matchedTe.Items) != 1 {
        t.Fatalf("Expected exactly one location item to be matched: %v\n", matchedTe.Items)
    }

    gr := matchedTe.Items[0].(*geoindex.GeographicRecord)

    expectedLatitude := float64(2.3)
    if gr.Latitude != expectedLatitude {
        t.Fatalf("Matched latitude not correct: [%.10f] != [%.10f]", gr.Latitude, expectedLatitude)
    }

    expectedLongitude := float64(20.3)
    if gr.Longitude != expectedLongitude {
        t.Fatalf("Matched longitude not correct: [%.10f] != [%.10f]", gr.Longitude, expectedLongitude)
    }
}

func TestFindGroups_FindLocationByTime_JustAfterLocationRecord(t *testing.T) {
    locationTs := getTestLocationTs()

    fg := NewFindGroups(locationTs, nil, nil)

    imageTimestamp := epochUtc.Add(time.Hour*1 + time.Minute*11)

    imageTe := timeindex.TimeEntry{
        Time:  imageTimestamp,
        Items: nil,
    }

    matchedTe, err := fg.findLocationByTimeBestGuess(imageTe)
    log.PanicIf(err)

    expectedLocationTimestamp := epochUtc.Add(time.Hour*1 + time.Minute*10)

    if matchedTe.Time != expectedLocationTimestamp {
        t.Fatalf("The matched location timestamp is not correct: [%s] != [%s]", matchedTe.Time, expectedLocationTimestamp)
    } else if len(matchedTe.Items) != 1 {
        t.Fatalf("Expected exactly one location item to be matched: %v\n", matchedTe.Items)
    }

    gr := matchedTe.Items[0].(*geoindex.GeographicRecord)

    expectedLatitude := float64(2.3)
    if gr.Latitude != expectedLatitude {
        t.Fatalf("Matched latitude not correct: [%.10f] != [%.10f]", gr.Latitude, expectedLatitude)
    }

    expectedLongitude := float64(20.3)
    if gr.Longitude != expectedLongitude {
        t.Fatalf("Matched longitude not correct: [%.10f] != [%.10f]", gr.Longitude, expectedLongitude)
    }
}

func TestFindGroups_FindLocationByTime_RoundUpToLocationRecord(t *testing.T) {
    locationTs := getTestLocationTs()

    fg := NewFindGroups(locationTs, nil, nil)

    imageTimestamp := epochUtc.Add(time.Hour*3 + time.Minute*16)

    imageTe := timeindex.TimeEntry{
        Time:  imageTimestamp,
        Items: nil,
    }

    matchedTe, err := fg.findLocationByTimeBestGuess(imageTe)
    log.PanicIf(err)

    expectedLocationTimestamp := epochUtc.Add(time.Hour*3 + time.Minute*20)

    if matchedTe.Time != expectedLocationTimestamp {
        t.Fatalf("The matched location timestamp is not correct: [%s] != [%s]", matchedTe.Time, expectedLocationTimestamp)
    } else if len(matchedTe.Items) != 1 {
        t.Fatalf("Expected exactly one location item to be matched: %v\n", matchedTe.Items)
    }

    gr := matchedTe.Items[0].(*geoindex.GeographicRecord)

    expectedLatitude := float64(4.3)
    if gr.Latitude != expectedLatitude {
        t.Fatalf("Matched latitude not correct: [%.10f] != [%.10f]", gr.Latitude, expectedLatitude)
    }

    expectedLongitude := float64(40.3)
    if gr.Longitude != expectedLongitude {
        t.Fatalf("Matched longitude not correct: [%.10f] != [%.10f]", gr.Longitude, expectedLongitude)
    }
}

func TestFindGroups_FindLocationByTime_RoundDownToLocationRecord(t *testing.T) {
    locationTs := getTestLocationTs()

    fg := NewFindGroups(locationTs, nil, nil)

    imageTimestamp := epochUtc.Add(time.Hour*3 + time.Minute*14)

    imageTe := timeindex.TimeEntry{
        Time:  imageTimestamp,
        Items: nil,
    }

    matchedTe, err := fg.findLocationByTimeBestGuess(imageTe)
    log.PanicIf(err)

    expectedLocationTimestamp := epochUtc.Add(time.Hour*3 + time.Minute*10)

    if matchedTe.Time != expectedLocationTimestamp {
        t.Fatalf("The matched location timestamp is not correct: [%s] != [%s]", matchedTe.Time, expectedLocationTimestamp)
    } else if len(matchedTe.Items) != 1 {
        t.Fatalf("Expected exactly one location item to be matched: %v\n", matchedTe.Items)
    }

    gr := matchedTe.Items[0].(*geoindex.GeographicRecord)

    expectedLatitude := float64(4.2)
    if gr.Latitude != expectedLatitude {
        t.Fatalf("Matched latitude not correct: [%.10f] != [%.10f]", gr.Latitude, expectedLatitude)
    }

    expectedLongitude := float64(40.2)
    if gr.Longitude != expectedLongitude {
        t.Fatalf("Matched longitude not correct: [%.10f] != [%.10f]", gr.Longitude, expectedLongitude)
    }
}

func TestFindGroups_FindLocationByTime_NoMatch(t *testing.T) {
    locationTs := getTestLocationTs()

    fg := NewFindGroups(locationTs, nil, nil)

    imageTimestamp := epochUtc.Add(oneDay*4 + time.Hour*0 + time.Minute*0)

    imageTe := timeindex.TimeEntry{
        Time:  imageTimestamp,
        Items: nil,
    }

    _, err := fg.findLocationByTimeBestGuess(imageTe)
    if err != ErrNoNearLocationRecord {
        t.Fatalf("Didn't get error as expected for no matched location.")
    }
}

func getTestImageTs(models map[string]string) timeindex.TimeSlice {
    timeBase := epochUtc

    imageTi := geoindex.NewTimeIndex()

    // Note that we also mess-up the order in order to test that it's internally
    // sorted.

    timeSeries := map[string]struct {
        timestamp time.Time
        latitude  float64
        longitude float64
    }{
        "file01.jpg": {timeBase.Add(time.Hour*0 + time.Minute*1), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file00.jpg": {timeBase.Add(time.Hour*0 + time.Minute*0), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file04.jpg": {timeBase.Add(time.Hour*0 + time.Minute*4), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file03.jpg": {timeBase.Add(time.Hour*0 + time.Minute*3), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file02.jpg": {timeBase.Add(time.Hour*0 + time.Minute*2), chicagoCoordinates[0], chicagoCoordinates[1]},

        "file11.jpg": {timeBase.Add(time.Hour*1 + time.Minute*5), detroitCoordinates[0], detroitCoordinates[1]},
        "file10.jpg": {timeBase.Add(time.Hour*1 + time.Minute*0), detroitCoordinates[0], detroitCoordinates[1]},
        "file14.jpg": {timeBase.Add(time.Hour*1 + time.Minute*20), detroitCoordinates[0], detroitCoordinates[1]},
        "file13.jpg": {timeBase.Add(time.Hour*1 + time.Minute*15), detroitCoordinates[0], detroitCoordinates[1]},
        "file12.jpg": {timeBase.Add(time.Hour*1 + time.Minute*10), detroitCoordinates[0], detroitCoordinates[1]},

        "file21.jpg": {timeBase.Add(time.Hour*2 + time.Minute*1), nycCoordinates[0], nycCoordinates[1]},
        "file20.jpg": {timeBase.Add(time.Hour*2 + time.Minute*0), nycCoordinates[0], nycCoordinates[1]},
        "file24.jpg": {timeBase.Add(time.Hour*2 + time.Minute*4), nycCoordinates[0], nycCoordinates[1]},
        "file23.jpg": {timeBase.Add(time.Hour*2 + time.Minute*3), nycCoordinates[0], nycCoordinates[1]},
        "file22.jpg": {timeBase.Add(time.Hour*2 + time.Minute*2), nycCoordinates[0], nycCoordinates[1]},

        "file31.jpg": {timeBase.Add(time.Hour*3 + time.Minute*10), sydneyCoordinates[0], sydneyCoordinates[1]},
        "file30.jpg": {timeBase.Add(time.Hour*3 + time.Minute*0), sydneyCoordinates[0], sydneyCoordinates[1]},
        "file34.jpg": {timeBase.Add(time.Hour*3 + time.Minute*40), sydneyCoordinates[0], sydneyCoordinates[1]},
        "file33.jpg": {timeBase.Add(time.Hour*3 + time.Minute*30), sydneyCoordinates[0], sydneyCoordinates[1]},
        "file32.jpg": {timeBase.Add(time.Hour*3 + time.Minute*20), sydneyCoordinates[0], sydneyCoordinates[1]},

        "file41.jpg": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*10), joCoordinates[0], joCoordinates[1]},
        "file40.jpg": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*0), joCoordinates[0], joCoordinates[1]},
        "file44.jpg": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*40), joCoordinates[0], joCoordinates[1]},
        "file43.jpg": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*30), joCoordinates[0], joCoordinates[1]},
        "file42.jpg": {timeBase.Add(oneDay*2 + time.Hour*0 + time.Minute*20), joCoordinates[0], joCoordinates[1]},

        "file51.jpg": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*10), dresdenCoordinates[0], dresdenCoordinates[1]},
        "file50.jpg": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*0), dresdenCoordinates[0], dresdenCoordinates[1]},
        "file54.jpg": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*40), dresdenCoordinates[0], dresdenCoordinates[1]},
        "file53.jpg": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*30), dresdenCoordinates[0], dresdenCoordinates[1]},
        "file52.jpg": {timeBase.Add(oneDay*6 + time.Hour*0 + time.Minute*20), dresdenCoordinates[0], dresdenCoordinates[1]},
    }

    for filepath, x := range timeSeries {
        cameraModel := "some model"
        if models != nil {
            cameraModel = models[filepath]
        }

        im := geoindex.ImageMetadata{
            CameraModel: cameraModel,
        }

        gr := geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, filepath, x.timestamp, true, x.latitude, x.longitude, im)
        imageTi.AddWithRecord(gr)
    }

    return imageTi.Series()
}

func checkGroup(fg *FindGroups, finishedGroupKey GroupKey, finishedGroup []*geoindex.GeographicRecord, expectedTimeKey time.Time, expectedCountry, expectedCity string, expectedFilenames []string) {
    cityLookup := fg.NearestCityIndex()
    cityRecord := cityLookup[finishedGroupKey.NearestCityKey]
    if cityRecord.Country != expectedCountry || cityRecord.City != expectedCity {
        log.Panicf("Matched city not correct:\nACTUAL: [%s] [%s]\nEXPECTED: [%s] [%s]", cityRecord.City, cityRecord.Country, expectedCity, expectedCountry)
    }

    if finishedGroupKey.CameraModel != "some model" {
        log.Panicf("Camera model not correct: [%s]", finishedGroupKey.CameraModel)
    }

    if len(finishedGroup) != len(expectedFilenames) {
        log.Panicf("Group is not the right size: (%d) != (%d)", len(finishedGroup), len(expectedFilenames))
    }

    for i, gr := range finishedGroup {
        if gr.Filepath != expectedFilenames[i] {
            for j, actualGr := range finishedGroup {
                fmt.Printf("(%d): [%s]\n", j, actualGr.Filepath)
            }

            log.Panicf("File-path (%d) in group is not correct: [%s] != [%s]", i, gr.Filepath, expectedFilenames[i])
        }
    }

    if finishedGroupKey.TimeKey != expectedTimeKey {
        log.Panicf("Time-key not correct: [%s] != [%s]\n", finishedGroupKey.TimeKey, expectedTimeKey)
    }
}

func TestFindGroups_FindNext_ImagesWithLocations_SameModel(t *testing.T) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)

            t.Fatalf("Test error.")
        }
    }()

    // locationIndex is just a non-empty index. We won't use it, but it needs to
    // be present with at least one entry.
    locationTi := geoindex.NewTimeIndex()

    gr := geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file1", epochUtc, true, 1.1, 10.1, nil)
    locationTi.AddWithRecord(gr)

    imageTs := getTestImageTs(nil)

    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    ci, err := GetCityIndex("", countriesFilepath, citiesFilepath, nil, false)
    log.PanicIf(err)

    locationTs := locationTi.Series()
    fg := NewFindGroups(locationTs, imageTs, ci)

    finishedGroupKey, finishedGroup, err := fg.FindNext()
    log.PanicIf(err)

    alignedTimeKey1 := time.Time{}
    err = alignedTimeKey1.UnmarshalText([]byte("1970-01-01T00:00:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        alignedTimeKey1,
        "United States", "Chicago",
        []string{"file00.jpg", "file01.jpg", "file02.jpg", "file03.jpg", "file04.jpg"})

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    alignedTimeKey2 := time.Time{}
    err = alignedTimeKey2.UnmarshalText([]byte("1970-01-01T01:00:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        alignedTimeKey2,
        "United States", "Detroit",
        []string{"file10.jpg", "file11.jpg", "file12.jpg", "file13.jpg", "file14.jpg"})

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    alignedTimeKey3 := time.Time{}
    err = alignedTimeKey3.UnmarshalText([]byte("1970-01-01T02:00:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        alignedTimeKey3,
        "United States", "New York City",
        []string{"file20.jpg", "file21.jpg", "file22.jpg", "file23.jpg", "file24.jpg"})

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    alignedTimeKey4 := time.Time{}
    err = alignedTimeKey4.UnmarshalText([]byte("1970-01-01T03:00:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        alignedTimeKey4,
        "Australia", "Sydney",
        []string{"file30.jpg", "file31.jpg", "file32.jpg", "file33.jpg", "file34.jpg"})

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    alignedTimeKey5 := time.Time{}
    err = alignedTimeKey5.UnmarshalText([]byte("1970-01-03T00:00:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        alignedTimeKey5,
        "South Africa", "Johannesburg",
        []string{"file40.jpg", "file41.jpg", "file42.jpg", "file43.jpg", "file44.jpg"})

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    alignedTimeKey6 := time.Time{}
    err = alignedTimeKey6.UnmarshalText([]byte("1970-01-07T00:00:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        alignedTimeKey6,
        "Germany", "Dresden",
        []string{"file50.jpg", "file51.jpg", "file52.jpg", "file53.jpg", "file54.jpg"})

    _, _, err = fg.FindNext()
    if err != ErrNoMoreGroups {
        t.Fatalf("Expected no-more-groups error.")
    }
}

func TestFindGroups_FindNext_ImagesWithLocations_DifferentModels_AlignedWithTimeBoundaries(t *testing.T) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)

            panic(err)
        }
    }()

    // locationIndex is just a non-empty index. We won't use it, but it needs to
    // be present with at least one entry.
    locationTi := geoindex.NewTimeIndex()

    gr := geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file1", epochUtc, true, 1.1, 10.1, nil)
    locationTi.AddWithRecord(gr)

    models := map[string]string{
        "file01.jpg": "model1",
        "file00.jpg": "model1",
        "file04.jpg": "model1",
        "file03.jpg": "model1",
        "file02.jpg": "model1",

        "file11.jpg": "model2",
        "file10.jpg": "model2",
        "file14.jpg": "model2",
        "file13.jpg": "model2",
        "file12.jpg": "model2",

        "file21.jpg": "model3",
        "file20.jpg": "model3",
        "file24.jpg": "model3",
        "file23.jpg": "model3",
        "file22.jpg": "model3",

        "file31.jpg": "model4",
        "file30.jpg": "model4",
        "file34.jpg": "model4",
        "file33.jpg": "model4",
        "file32.jpg": "model4",

        "file41.jpg": "model5",
        "file40.jpg": "model5",
        "file44.jpg": "model5",
        "file43.jpg": "model5",
        "file42.jpg": "model5",

        "file51.jpg": "model6",
        "file50.jpg": "model6",
        "file54.jpg": "model6",
        "file53.jpg": "model6",
        "file52.jpg": "model6",
    }

    imageTs := getTestImageTs(models)

    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    ci, err := GetCityIndex("", countriesFilepath, citiesFilepath, nil, false)
    log.PanicIf(err)

    locationTs := locationTi.Series()
    fg := NewFindGroups(locationTs, imageTs, ci)

    // Because of the internal mechanics of the algorithm, we'll get the groups
    // back in an unpredictable order. It won't even be consistent from one
    // execution to the next. So, store first and check later.

    groups := make(map[GroupKey]int, 5)

    finishedGroupKey, finishedGroup, err := fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    _, _, err = fg.FindNext()
    if err != ErrNoMoreGroups {
        t.Fatalf("Expected no-more-groups error.")
    }

    group1Timekey := time.Time{}
    err = group1Timekey.UnmarshalText([]byte("1970-01-01T00:00:00Z"))
    log.PanicIf(err)

    group2Timekey := time.Time{}
    err = group2Timekey.UnmarshalText([]byte("1970-01-01T01:00:00Z"))
    log.PanicIf(err)

    group4Timekey := time.Time{}
    err = group4Timekey.UnmarshalText([]byte("1970-01-01T03:00:00Z"))
    log.PanicIf(err)

    group6Timekey := time.Time{}
    err = group6Timekey.UnmarshalText([]byte("1970-01-07T00:00:00Z"))
    log.PanicIf(err)

    group5Timekey := time.Time{}
    err = group5Timekey.UnmarshalText([]byte("1970-01-03T00:00:00Z"))
    log.PanicIf(err)

    group3Timekey := time.Time{}
    err = group3Timekey.UnmarshalText([]byte("1970-01-01T02:00:00Z"))
    log.PanicIf(err)

    expectedGroups := map[GroupKey]int{
        GroupKey{TimeKey: group1Timekey, NearestCityKey: "GeoNames,4887398", CameraModel: "model1"}: 5,
        GroupKey{TimeKey: group2Timekey, NearestCityKey: "GeoNames,4990729", CameraModel: "model2"}: 5,
        GroupKey{TimeKey: group4Timekey, NearestCityKey: "GeoNames,2147714", CameraModel: "model4"}: 5,
        GroupKey{TimeKey: group6Timekey, NearestCityKey: "GeoNames,2935022", CameraModel: "model6"}: 5,
        GroupKey{TimeKey: group5Timekey, NearestCityKey: "GeoNames,993800", CameraModel: "model5"}:  5,
        GroupKey{TimeKey: group3Timekey, NearestCityKey: "GeoNames,5128581", CameraModel: "model3"}: 5,
    }

    if reflect.DeepEqual(groups, expectedGroups) == false {
        for gk, groupSize := range groups {
            fmt.Printf("> %s (%d)\n", gk, groupSize)
        }

        t.Fatalf("The correct groups weren't returned.")
    }
}

func TestFindGroups_FindNext_ImagesWithLocations_DifferentModels_NotAlignedWithTimeBoundaries(t *testing.T) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)

            panic(err)
        }
    }()

    // locationIndex is just a non-empty index. We won't use it, but it needs to
    // be present with at least one entry.
    locationTi := geoindex.NewTimeIndex()

    gr := geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file1", epochUtc, true, 1.1, 10.1, nil)
    locationTi.AddWithRecord(gr)

    models := map[string]string{
        "file01.jpg": "model1",
        "file00.jpg": "model1",
        "file04.jpg": "model1",
        "file03.jpg": "model2",
        "file02.jpg": "model2",

        "file11.jpg": "model3",
        "file10.jpg": "model3",
        "file14.jpg": "model4",
        "file13.jpg": "model4",
        "file12.jpg": "model4",

        "file21.jpg": "model4",
        "file20.jpg": "model4",
        "file24.jpg": "model5",
        "file23.jpg": "model5",
        "file22.jpg": "model5",

        "file31.jpg": "model5",
        "file30.jpg": "model5",
        "file34.jpg": "model5",
        "file33.jpg": "model5",
        "file32.jpg": "model5",

        "file41.jpg": "model5",
        "file40.jpg": "model5",
        "file44.jpg": "model5",
        "file43.jpg": "model5",
        "file42.jpg": "model5",

        "file51.jpg": "model6",
        "file50.jpg": "model6",
        "file54.jpg": "model6",
        "file53.jpg": "model6",
        "file52.jpg": "model6",
    }

    imageTs := getTestImageTs(models)

    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    ci, err := GetCityIndex("", countriesFilepath, citiesFilepath, nil, false)
    log.PanicIf(err)

    locationTs := locationTi.Series()
    fg := NewFindGroups(locationTs, imageTs, ci)

    // Because of the internal mechanics of the algorithm, we'll get the groups
    // back in an unpredictable order. It won't even be consistent from one
    // execution to the next. So, store first and check later.

    groups := make(map[GroupKey]int, 5)

    finishedGroupKey, finishedGroup, err := fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    groups[finishedGroupKey] = len(finishedGroup)

    _, _, err = fg.FindNext()
    if err != ErrNoMoreGroups {
        t.Fatalf("Expected no-more-groups error.")
    }

    group1Timekey := time.Time{}
    err = group1Timekey.UnmarshalText([]byte("1970-01-01T00:00:00Z"))
    log.PanicIf(err)

    group2aTimekey := time.Time{}
    err = group2aTimekey.UnmarshalText([]byte("1970-01-01T01:00:00Z"))
    log.PanicIf(err)

    group2bTimekey := time.Time{}
    err = group2bTimekey.UnmarshalText([]byte("1970-01-01T01:10:00Z"))
    log.PanicIf(err)

    group3Timekey := time.Time{}
    err = group3Timekey.UnmarshalText([]byte("1970-01-01T02:00:00Z"))
    log.PanicIf(err)

    group4Timekey := time.Time{}
    err = group4Timekey.UnmarshalText([]byte("1970-01-01T03:00:00Z"))
    log.PanicIf(err)

    group5Timekey := time.Time{}
    err = group5Timekey.UnmarshalText([]byte("1970-01-03T00:00:00Z"))
    log.PanicIf(err)

    group6Timekey := time.Time{}
    err = group6Timekey.UnmarshalText([]byte("1970-01-07T00:00:00Z"))
    log.PanicIf(err)

    expectedGroups := map[GroupKey]int{
        GroupKey{TimeKey: group1Timekey, NearestCityKey: "GeoNames,4887398", CameraModel: "model1"}: 3,
        GroupKey{TimeKey: group1Timekey, NearestCityKey: "GeoNames,4887398", CameraModel: "model2"}: 2,

        GroupKey{TimeKey: group2aTimekey, NearestCityKey: "GeoNames,4990729", CameraModel: "model3"}: 2,
        GroupKey{TimeKey: group2bTimekey, NearestCityKey: "GeoNames,4990729", CameraModel: "model4"}: 3,

        GroupKey{TimeKey: group3Timekey, NearestCityKey: "GeoNames,5128581", CameraModel: "model4"}: 2,
        GroupKey{TimeKey: group3Timekey, NearestCityKey: "GeoNames,5128581", CameraModel: "model5"}: 3,

        GroupKey{TimeKey: group4Timekey, NearestCityKey: "GeoNames,2147714", CameraModel: "model5"}: 5,

        GroupKey{TimeKey: group5Timekey, NearestCityKey: "GeoNames,993800", CameraModel: "model5"}: 5,

        GroupKey{TimeKey: group6Timekey, NearestCityKey: "GeoNames,2935022", CameraModel: "model6"}: 5,
    }

    if reflect.DeepEqual(groups, expectedGroups) == false {
        for gk, groupSize := range groups {
            fmt.Printf("  ACTUAL> %s (%d)\n", gk, groupSize)
        }

        for gk, groupSize := range expectedGroups {
            fmt.Printf("EXPECTED> %s (%d)\n", gk, groupSize)
        }

        for gk, count := range groups {
            expectedCount, found := expectedGroups[gk]
            if found == false {
                fmt.Printf("GK not expected: %s (%d)\n", gk, count)
            } else {
                if expectedCount != count {
                    fmt.Printf("Count doesn't match for [%s]: (%d) != (%d)\n", gk, count, expectedCount)
                }

                delete(expectedGroups, gk)
            }
        }

        for gk, groupSize := range expectedGroups {
            fmt.Printf("Expected but not present: %s (%d)\n", gk, groupSize)
        }

        t.Fatalf("The correct groups weren't returned.")
    }
}

func TestFindGroups_FindNext_ImagesWithoutLocations(t *testing.T) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)

            panic(err)
        }
    }()

    // This just tests for whether the locations are assigned properly with
    // images that don't already have them. The rest of the mechanics are
    // already covered in other tests.

    locationTi := geoindex.NewTimeIndex()

    gr := geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*0), true, chicagoCoordinates[0], chicagoCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*5), true, chicagoCoordinates[0], chicagoCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*10), true, chicagoCoordinates[0], chicagoCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*15), true, chicagoCoordinates[0], chicagoCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*20), true, chicagoCoordinates[0], chicagoCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*1+time.Minute*0), true, detroitCoordinates[0], detroitCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*1+time.Minute*5), true, detroitCoordinates[0], detroitCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*1+time.Minute*10), true, detroitCoordinates[0], detroitCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*1+time.Minute*15), true, detroitCoordinates[0], detroitCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*1+time.Hour*1+time.Minute*20), true, detroitCoordinates[0], detroitCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*3+time.Hour*0+time.Minute*0), true, nycCoordinates[0], nycCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*3+time.Hour*0+time.Minute*5), true, nycCoordinates[0], nycCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*3+time.Hour*0+time.Minute*10), true, nycCoordinates[0], nycCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*3+time.Hour*0+time.Minute*15), true, nycCoordinates[0], nycCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file.gpx", epochUtc.Add(oneDay*3+time.Hour*0+time.Minute*20), true, nycCoordinates[0], nycCoordinates[1], nil)
    locationTi.AddWithRecord(gr)

    imageTi := geoindex.NewTimeIndex()

    im := geoindex.ImageMetadata{
        CameraModel: "some model",
    }

    // An exact match with one of the location records.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image1.jpg", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*10), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // Just before a known location record.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image2.jpg", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*9), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // Just after a known location record.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image3.jpg", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*11), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // Too far before a known location record to much.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image4.jpg", epochUtc.Add(oneDay*1+time.Hour*0+time.Minute*45), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // Too far after a known location record to much.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image5.jpg", epochUtc.Add(oneDay*1+time.Hour*1+time.Minute*45), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // No match before the beginning of history.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image6.jpg", epochUtc.Add(oneDay*0+time.Hour*0+time.Minute*0), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // No match within a large gap of history.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image7.jpg", epochUtc.Add(oneDay*2+time.Hour*0+time.Minute*0), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // No match after the end of history.
    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image8.jpg", epochUtc.Add(oneDay*5+time.Hour*0+time.Minute*0), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    // More matches.

    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image9.jpg", epochUtc.Add(oneDay*1+time.Hour*1+time.Minute*11), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    gr = geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, "image10.jpg", epochUtc.Add(oneDay*3+time.Hour*0+time.Minute*11), false, 0, 0, im)
    imageTi.AddWithRecord(gr)

    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    ci, err := GetCityIndex("", countriesFilepath, citiesFilepath, nil, false)
    log.PanicIf(err)

    locationTs := locationTi.Series()
    imageTs := imageTi.Series()

    fg := NewFindGroups(locationTs, imageTs, ci)

    // Chicago

    finishedGroupKey, finishedGroup, err := fg.FindNext()
    log.PanicIf(err)

    timeKey := time.Time{}
    err = timeKey.UnmarshalText([]byte("1970-01-02T00:00:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        timeKey,
        "United States", "Chicago",
        []string{"image2.jpg", "image1.jpg", "image3.jpg"})

    for i, gr := range finishedGroup {
        if gr.Latitude != chicagoCoordinates[0] || gr.Longitude != chicagoCoordinates[1] {
            t.Fatalf("Did not assign and match correctly (%d).", i)
        }
    }

    // Detroit

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    timeKey = time.Time{}
    err = timeKey.UnmarshalText([]byte("1970-01-02T01:10:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        timeKey,
        "United States", "Detroit",
        []string{"image9.jpg"})

    if finishedGroup[0].Latitude != detroitCoordinates[0] || finishedGroup[0].Longitude != detroitCoordinates[1] {
        t.Fatalf("Did not assign and match correctly (1).")
    }

    // NYC

    finishedGroupKey, finishedGroup, err = fg.FindNext()
    log.PanicIf(err)

    timeKey = time.Time{}
    err = timeKey.UnmarshalText([]byte("1970-01-04T00:10:00Z"))
    log.PanicIf(err)

    checkGroup(
        fg,
        finishedGroupKey,
        finishedGroup,
        timeKey,
        "United States", "New York City",
        []string{"image10.jpg"})

    if finishedGroup[0].Latitude != nycCoordinates[0] || finishedGroup[0].Longitude != nycCoordinates[1] {
        t.Fatalf("Did not assign and match correctly (2).")
    }

    // Check unassigned images.

    expectedUnassignedFiles := []string{
        "image6.jpg",
        "image4.jpg",
        "image5.jpg",
        "image7.jpg",
        "image8.jpg",
    }

    unassignedRecords := fg.UnassignedRecords()
    actualUnassignedFiles := make([]string, len(unassignedRecords))
    for i, ur := range unassignedRecords {
        if ur.Reason != SkipReasonNoNearLocationRecord {
            t.Fatalf("Image (%d) [%s] has unexpected reason: [%s] != [%s]", i, ur, ur.Reason, SkipReasonNoNearLocationRecord)
        }

        actualUnassignedFiles[i] = ur.Geographic.Filepath
    }

    if reflect.DeepEqual(actualUnassignedFiles, expectedUnassignedFiles) == false {
        for i, ur := range actualUnassignedFiles {
            fmt.Printf("(%d): %s\n", i, ur)
        }

        t.Fatalf("Unassigned files are not correct.")
    }
}

func getExampleLocationTs() timeindex.TimeSlice {
    locationTi := geoindex.NewTimeIndex()

    gr := geoindex.NewGeographicRecord(geoindex.SourceGeographicGpx, "file1", epochUtc, true, 1.1, 10.1, nil)
    locationTi.AddWithRecord(gr)

    return locationTi.Series()
}

func getExampleImageTs() timeindex.TimeSlice {
    imageTi := geoindex.NewTimeIndex()

    timeSeries := map[string]struct {
        timestamp time.Time
        latitude  float64
        longitude float64
    }{
        "file01.jpg": {epochUtc.Add(time.Hour*0 + time.Minute*1), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file00.jpg": {epochUtc.Add(time.Hour*0 + time.Minute*0), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file04.jpg": {epochUtc.Add(time.Hour*0 + time.Minute*4), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file03.jpg": {epochUtc.Add(time.Hour*0 + time.Minute*3), chicagoCoordinates[0], chicagoCoordinates[1]},
        "file02.jpg": {epochUtc.Add(time.Hour*0 + time.Minute*2), chicagoCoordinates[0], chicagoCoordinates[1]},
    }

    im := geoindex.ImageMetadata{
        CameraModel: "some model",
    }

    for filepath, x := range timeSeries {
        gr := geoindex.NewGeographicRecord(geoindex.SourceImageJpeg, filepath, x.timestamp, true, x.latitude, x.longitude, im)
        imageTi.AddWithRecord(gr)
    }

    return imageTi.Series()
}

func ExampleFindGroups_FindNext() {
    citiesFilepath := path.Join(testAssetsPath, "allCountries.txt.multiple_major_cities_handpicked")
    countriesFilepath := path.Join(testAssetsPath, "countryInfo.txt")

    cityIndex, err := GetCityIndex("", countriesFilepath, citiesFilepath, nil, false)
    log.PanicIf(err)

    // We use a couple of fake indices for the purpose of the example.
    locationTs := getExampleLocationTs()
    imageTs := getExampleImageTs()

    // Create FindGroup struct.
    fg := NewFindGroups(locationTs, imageTs, cityIndex)

    // Identify groups.

    for {
        finishedGroupKey, finishedGroup, err := fg.FindNext()
        if err != nil {
            if err == ErrNoMoreGroups {
                break
            }

            log.Panic(err)
        }

        fmt.Printf("GROUP KEY: %s\n", finishedGroupKey)

        nearestCityIndex := fg.NearestCityIndex()
        cityRecord := nearestCityIndex[finishedGroupKey.NearestCityKey]

        fmt.Printf("CITY: %s\n", cityRecord)

        for i, gr := range finishedGroup {
            fmt.Printf("(%d): %s\n", i, gr)
        }
    }

    // This is here for posterity. We won't have any ungrouped images in this
    // example.
    unassignedRecords := fg.UnassignedRecords()
    for _, ur := range unassignedRecords {
        fmt.Printf("UNASSIGNED: %s\n", ur)
    }

    // Output:
    // GROUP KEY: GroupKey<TIME-KEY=[1970-01-01T00:00:00Z] NEAREST-CITY=[GeoNames,4887398] CAMERA-MODEL=[some model]>
    // CITY: CityRecord<ID=[4887398] COUNTRY=[United States] PROVINCE-OR-STATE=[IL] CITY=[Chicago] POP=(2720546) LAT=(41.8500300000) LON=(-87.6500500000) S2=[880e2c50c345d397]>
    // (0): GeographicRecord<F=[file00.jpg] LAT=[41.850030] LON=[-87.650050] CELL=[9803822164217287575]>
    // (1): GeographicRecord<F=[file01.jpg] LAT=[41.850030] LON=[-87.650050] CELL=[9803822164217287575]>
    // (2): GeographicRecord<F=[file02.jpg] LAT=[41.850030] LON=[-87.650050] CELL=[9803822164217287575]>
    // (3): GeographicRecord<F=[file03.jpg] LAT=[41.850030] LON=[-87.650050] CELL=[9803822164217287575]>
    // (4): GeographicRecord<F=[file04.jpg] LAT=[41.850030] LON=[-87.650050] CELL=[9803822164217287575]>
}

// TODO(dustin): !! Add test for `findLocationByTimeWithSparseLocations`.
