package geoautogroup

import (
    "testing"
    "time"

    "github.com/dsoprea/go-geographic-index"
)

func TestInitBufferedGroup(t *testing.T) {
    now1 := time.Now()

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)

    nearestCityKey := "nearest city"
    bg := initBufferedGroup(nearestCityKey, gr)

    timeKey := getGeographicRecordTimeKey(gr)
    if bg.firstTimeKey != timeKey {
        t.Fatalf("First time-key not correct.")
    } else if bg.lastTimeKey != timeKey {
        t.Fatalf("Last time-key not correct.")
    }

    if len(bg.images) != 1 {
        t.Fatalf("Expected exactly one image.")
    }

    bi := bg.images[0]
    if bi.gr != gr {
        t.Fatalf("GeographicRecord record not correct.")
    } else if bi.nearestCityKey != nearestCityKey {
        t.Fatalf("nearestCityKey not correct.")
    }
}

func TestBufferedGroup_pushImage(t *testing.T) {
    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)

    nearestCityKey1 := "nearest city"
    bg := initBufferedGroup(nearestCityKey1, gr1)

    gr2 := geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, nil)

    nearestCityKey2 := "nearest city 2"
    bg.pushImage(nearestCityKey2, gr2)

    timeKey1 := getGeographicRecordTimeKey(gr1)
    timeKey2 := getGeographicRecordTimeKey(gr2)

    if bg.firstTimeKey != timeKey1 {
        t.Fatalf("First time-key not correct.")
    } else if bg.lastTimeKey != timeKey2 {
        t.Fatalf("Last time-key not correct.")
    }

    if len(bg.images) != 2 {
        t.Fatalf("Expected exactly two images.")
    }

    bi1 := bg.images[0]
    if bi1.gr != gr1 {
        t.Fatalf("GeographicRecord (1) record not correct.")
    } else if bi1.nearestCityKey != nearestCityKey1 {
        t.Fatalf("nearestCityKey1 not correct.")
    }

    bi2 := bg.images[1]
    if bi2.gr != gr2 {
        t.Fatalf("GeographicRecord (2) record not correct.")
    } else if bi2.nearestCityKey != nearestCityKey2 {
        t.Fatalf("nearestCityKey2 not correct.")
    }
}

func TestBufferedGroup_haveCompleteGroup_true(t *testing.T) {
    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)
    bg := initBufferedGroup("nearest city", gr)

    gr = geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, nil)
    bg.pushImage("nearest city 2", gr)

    if bg.haveCompleteGroup() == false {
        t.Fatalf("Expected that we'd have a complete group")
    }
}

func TestBufferedGroup_haveCompleteGroup_false(t *testing.T) {
    now1 := time.Now()

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)
    bg := initBufferedGroup("nearest city", gr)

    if bg.haveCompleteGroup() == true {
        t.Fatalf("Expected that we'd wouldn't have a complete group")
    }
}

func TestBufferedGroup_havePartialGroup_true(t *testing.T) {
    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)
    bg := initBufferedGroup("nearest city", gr)

    gr = geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, nil)
    bg.pushImage("nearest city 2", gr)

    if bg.haveCompleteGroup() == false {
        t.Fatalf("Expected that we'd have a complete group")
    } else if bg.havePartialGroup() == true {
        t.Fatalf("Expected that we'd wouldn't have a partial group")
    }
}

func TestBufferedGroup_havePartialGroup_false(t *testing.T) {
    now1 := time.Now()

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)
    bg := initBufferedGroup("nearest city", gr)

    if bg.haveCompleteGroup() == true {
        t.Fatalf("Expected that we'd wouldn't have a complete group")
    } else if bg.havePartialGroup() == false {
        t.Fatalf("Expected that we'd would have a partial group")
    }
}

func TestBufferedGroup_isEmpty_true(t *testing.T) {
    bg := &bufferedGroup{
        firstTimeKey: time.Time{},
        lastTimeKey:  time.Time{},
        images:       make([]*bufferedImage, 0),
    }

    if bg.isEmpty() == false {
        t.Fatalf("Expected to be empty.")
    }
}

func TestBufferedGroup_isEmpty_false(t *testing.T) {
    now1 := time.Now()

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)
    bg := initBufferedGroup("nearest city", gr)

    if bg.isEmpty() == true {
        t.Fatalf("Expected to not be empty.")
    }
}

func TestBufferedGroup_popPartialGroup_afterPopComplete(t *testing.T) {
    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)
    bg := initBufferedGroup("nearest city", gr1)

    gr2 := geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, nil)
    bg.pushImage("nearest city 2", gr2)

    if bg.haveCompleteGroup() == false {
        t.Fatalf("Expected that we'd have a complete group")
    }

    nearestCityKey, group := bg.popCompleteGroup()

    if nearestCityKey != "nearest city" {
        t.Fatalf("Nearest-city-key not correct.")
    } else if len(group) != 1 {
        t.Fatalf("Group size not correct.")
    }

    firstGroupedGr := group[0]

    if firstGroupedGr != gr1 {
        t.Fatalf("GeographicRecord not recovered correctly.")
    }

    if bg.haveCompleteGroup() == true {
        t.Fatalf("Expected that we'd no longer have a complete group")
    }
}

func TestBufferedGroup_popPartialGroup(t *testing.T) {
    now1 := time.Now()

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, nil)
    bg := initBufferedGroup("nearest city", gr1)

    if bg.haveCompleteGroup() == true {
        t.Fatalf("Expected that we wouldn't have a complete group")
    }

    nearestCityKey, group := bg.popPartialGroup()

    if nearestCityKey != "nearest city" {
        t.Fatalf("Nearest-city-key not correct.")
    } else if len(group) != 1 {
        t.Fatalf("Group size not correct.")
    }

    firstGroupedGr := group[0]

    if firstGroupedGr != gr1 {
        t.Fatalf("GeographicRecord not recovered correctly.")
    }
}

func TestNewIterativeGroupBuffers_empty(t *testing.T) {
    igb := newIterativeGroupBuffers()
    cameraModels := igb.bufferedCameraModels()

    if len(cameraModels) != 0 {
        t.Fatalf("Expected no models.")
    }
}

func TestNewIterativeGroupBuffers_nonempty(t *testing.T) {
    igb := newIterativeGroupBuffers()
    cameraModels := igb.bufferedCameraModels()

    if len(cameraModels) != 0 {
        t.Fatalf("Expected no models.")
    }

    metadata := geoindex.ImageMetadata{
        CameraModel: "some model",
    }

    now1 := time.Now()

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, metadata)
    igb.pushImage("nearest city", gr)

    cameraModels = igb.bufferedCameraModels()

    if len(cameraModels) != 1 {
        t.Fatalf("Expected there to be models.")
    } else if cameraModels[0] != "some model" {
        t.Fatalf("The wrong model was recorded.")
    }
}

func TestIterativeGroupBuffers_pushImage(t *testing.T) {
    igb := newIterativeGroupBuffers()

    metadata := geoindex.ImageMetadata{
        CameraModel: "some model",
    }

    now1 := time.Now()

    gr := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, metadata)
    igb.pushImage("nearest city", gr)

    if len(igb.groupsByCameraModel) != 1 {
        t.Fatalf("Exactly one buffered-group was not registered.")
    }

    bg, found := igb.groupsByCameraModel["some model"]
    if found == false {
        t.Fatalf("Buffered-group was not found")
    }

    timeKey := getGeographicRecordTimeKey(gr)

    if bg.firstTimeKey != timeKey {
        t.Fatalf("First time-key not correct.")
    } else if bg.lastTimeKey != timeKey {
        t.Fatalf("Last time-key not correct.")
    }

    if len(bg.images) != 1 {
        t.Fatalf("Expected exactly one image.")
    }

    bi1 := bg.images[0]
    if bi1.gr != gr {
        t.Fatalf("GeographicRecord record not correct.")
    } else if bi1.nearestCityKey != "nearest city" {
        t.Fatalf("nearestCityKey not correct.")
    }
}

func TestIterativeGroupBuffers_haveAnyCompleteGroups_JustComplete(t *testing.T) {
    igb := newIterativeGroupBuffers()

    metadata := geoindex.ImageMetadata{
        CameraModel: "some model",
    }

    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, metadata)
    igb.pushImage("nearest city", gr1)

    gr2 := geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, metadata)
    igb.pushImage("nearest city 2", gr2)

    cameraModel := igb.haveAnyCompleteGroups()
    if cameraModel != "some model" {
        t.Fatalf("Expected one complete group.")
    }

    cameraModel = igb.haveAnyPartialGroups()
    if cameraModel != "" {
        t.Fatalf("Expected no partial groups.")
    }
}

func TestIterativeGroupBuffers_haveAnyCompleteGroups_and_haveAnyPartialGroups(t *testing.T) {
    igb := newIterativeGroupBuffers()

    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    metadata1 := geoindex.ImageMetadata{
        CameraModel: "some model 1",
    }

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, metadata1)
    igb.pushImage("nearest city", gr1)

    gr2 := geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, metadata1)
    igb.pushImage("nearest city 2", gr2)

    metadata2 := geoindex.ImageMetadata{
        CameraModel: "some model 2",
    }

    gr3 := geoindex.NewGeographicRecord("source-name", "33.jpg", now1, true, 12.34, 34.56, metadata2)
    igb.pushImage("nearest city", gr3)

    cameraModel := igb.haveAnyCompleteGroups()
    if cameraModel != "some model 1" {
        t.Fatalf("Expected one complete group.")
    }

    cameraModel = igb.haveAnyPartialGroups()
    if cameraModel != "some model 2" {
        t.Fatalf("Expected one partial groups.")
    }
}

func TestIterativeGroupBuffers_haveAnyPartialGroups_JustPartial(t *testing.T) {
    igb := newIterativeGroupBuffers()

    metadata := geoindex.ImageMetadata{
        CameraModel: "some model",
    }

    now1 := time.Now()

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, metadata)
    igb.pushImage("nearest city", gr1)

    cameraModel := igb.haveAnyPartialGroups()
    if cameraModel != "some model" {
        t.Fatalf("Expected one partial group.")
    }

    cameraModel = igb.haveAnyCompleteGroups()
    if cameraModel != "" {
        t.Fatalf("Expected one complete group.")
    }
}

func TestIterativeGroupBuffers_popFirstCompleteGroup(t *testing.T) {
    igb := newIterativeGroupBuffers()

    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    metadata1 := geoindex.ImageMetadata{
        CameraModel: "some model 1",
    }

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, metadata1)
    igb.pushImage("nearest city", gr1)

    gr2 := geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, metadata1)
    igb.pushImage("nearest city 2", gr2)

    metadata2 := geoindex.ImageMetadata{
        CameraModel: "some model 2",
    }

    gr3 := geoindex.NewGeographicRecord("source-name", "33.jpg", now1, true, 12.34, 34.56, metadata2)
    igb.pushImage("nearest city", gr3)

    cameraModel := igb.haveAnyCompleteGroups()
    if cameraModel != "some model 1" {
        t.Fatalf("Expected one complete group.")
    }

    cameraModel = igb.haveAnyPartialGroups()
    if cameraModel != "some model 2" {
        t.Fatalf("Expected one partial groups.")
    }

    if len(igb.groupsByCameraModel) != 2 {
        t.Fatalf("Expected two different models to be registered.")
    }

    timeKey, nearestCityKey, cameraModel, images := igb.popFirstCompleteGroup()

    if len(igb.groupsByCameraModel) != 2 {
        t.Fatalf("Expected two different models to be registered.")
    }

    cameraModelEmpty := igb.haveAnyCompleteGroups()
    if cameraModelEmpty != "" {
        t.Fatalf("Expected no complete groups.")
    }

    expectedTimeKey := getGeographicRecordTimeKey(gr1)

    if timeKey != expectedTimeKey {
        t.Fatalf("Time-key of complete group is not correct.")
    } else if nearestCityKey != "nearest city" {
        t.Fatalf("nearestCityKey of complete group is not correct.")
    } else if cameraModel != "some model 1" {
        t.Fatalf("Camera model of complete group is not correct: [%s]", cameraModel)
    } else if len(images) != 1 {
        t.Fatalf("Exactly one image wasn't returned in complete group.")
    } else if images[0] != gr1 {
        t.Fatalf("The correct image wasn't returned in complete group.")
    }
}

func TestIterativeGroupBuffers_popFirstPartialGroup(t *testing.T) {
    igb := newIterativeGroupBuffers()

    now1 := time.Now()
    now2 := now1.Add(time.Second * TimeKeyAlignment)

    metadata1 := geoindex.ImageMetadata{
        CameraModel: "some model 1",
    }

    gr1 := geoindex.NewGeographicRecord("source-name", "11.jpg", now1, true, 12.34, 34.56, metadata1)
    igb.pushImage("nearest city", gr1)

    gr2 := geoindex.NewGeographicRecord("source-name", "22.jpg", now2, true, 12.34, 34.56, metadata1)
    igb.pushImage("nearest city 2", gr2)

    metadata2 := geoindex.ImageMetadata{
        CameraModel: "some model 2",
    }

    gr3 := geoindex.NewGeographicRecord("source-name", "33.jpg", now1, true, 12.34, 34.56, metadata2)
    igb.pushImage("nearest city", gr3)

    cameraModel := igb.haveAnyCompleteGroups()
    if cameraModel != "some model 1" {
        t.Fatalf("Expected one complete group.")
    }

    cameraModel = igb.haveAnyPartialGroups()
    if cameraModel != "some model 2" {
        t.Fatalf("Expected one partial groups.")
    }

    if len(igb.groupsByCameraModel) != 2 {
        t.Fatalf("Expected two different models to be registered.")
    }

    timeKey, nearestCityKey, cameraModel, images := igb.popFirstCompleteGroup()

    expectedTimeKey := getGeographicRecordTimeKey(gr1)

    if timeKey != expectedTimeKey {
        t.Fatalf("Time-key of complete group is not correct.")
    } else if nearestCityKey != "nearest city" {
        t.Fatalf("nearestCityKey of complete group is not correct.")
    } else if cameraModel != "some model 1" {
        t.Fatalf("Camera model of complete group is not correct: [%s]", cameraModel)
    } else if len(images) != 1 {
        t.Fatalf("Exactly one image wasn't returned in complete group.")
    } else if images[0] != gr1 {
        t.Fatalf("The correct image wasn't returned in complete group.")
    }

    _, _, cameraModelRecovered1, _ := igb.popFirstPartialGroup()

    // We can't depend on order since these are coming out of a dictionary. It's
    // enough for us to see the dictionary get smaller by one, anyway.
    if cameraModelRecovered1 != "some model 1" && cameraModelRecovered1 != "some model 2" {
        t.Fatalf("Popped partial group did not have an expected model (1).")
    }

    if len(igb.groupsByCameraModel) != 1 {
        t.Fatalf("Expected one model to be registered after popping the first complete group.")
    }

    _, _, cameraModelRecovered2, _ := igb.popFirstPartialGroup()

    // We can't depend on order since these are coming out of a dictionary. It's
    // enough for us to see the dictionary get smaller by one, anyway.
    if cameraModelRecovered2 != "some model 1" && cameraModelRecovered2 != "some model 2" {
        t.Fatalf("Popped partial group did not have an expected model (2).")
    }

    if len(igb.groupsByCameraModel) != 0 {
        t.Fatalf("Expected zero models to be registered after popping the second complete group.")
    }
}
