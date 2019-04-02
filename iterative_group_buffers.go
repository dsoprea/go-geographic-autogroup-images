package geoautogroup

import (
    "fmt"
    "path"
    "time"

    "github.com/dsoprea/go-logging"

    "github.com/dsoprea/go-geographic-index"
)

type bufferedImage struct {
    effectiveTimekey time.Time
    gr               *geoindex.GeographicRecord
    nearestCityKey   string
}

func (bi *bufferedImage) LocationTimekey() string {
    if bi.effectiveTimekey.IsZero() == true {
        log.Panicf("can not produce location-timekey if effective-timekey is zeroed: %v", bi)
    }

    return fmt.Sprintf("%s,%d", bi.nearestCityKey, bi.effectiveTimekey.Unix())
}

func newBufferedImage(nearestCityKey string, gr *geoindex.GeographicRecord, effectiveTimekey time.Time) *bufferedImage {
    if effectiveTimekey.IsZero() == true {
        effectiveTimekey = getGeographicRecordTimeKey(gr)
    }

    return &bufferedImage{
        effectiveTimekey: effectiveTimekey,
        gr:               gr,
        nearestCityKey:   nearestCityKey,
    }
}

type bufferedGroup struct {
    firstTimeKey time.Time
    lastTimeKey  time.Time
    images       []*bufferedImage

    // locationIndex is a map of nearest-cities to the first index at which they
    // appear.
    locationIndex map[string]int
}

func (bg *bufferedGroup) dump(printDetail bool) {
    fmt.Printf("BUFFERED GROUP\n")
    fmt.Printf("--------------\n")
    fmt.Printf("Have complete group? [%v]\n", bg.haveCompleteGroup())
    fmt.Printf("Have partial group? [%v]\n", bg.havePartialGroup())
    fmt.Printf("First time-key: [%s]\n", bg.firstTimeKey)
    fmt.Printf("Last time-key: [%s]\n", bg.lastTimeKey)
    fmt.Printf("Image count: (%d)\n", len(bg.images))

    if printDetail == true {
        fmt.Printf("\n")

        for i, bi := range bg.images {
            fmt.Printf("> Image (%d): EFF-TIME-KEY=[%s] CITY=[%s] FILEPATH=[%s]\n", i, bi.effectiveTimekey, bi.nearestCityKey, bi.gr.Filepath)
        }

        fmt.Printf("\n")
    }

    fmt.Printf("\n")
}

// haveCompleteGroup will return true if we have more than one time-key in the
// buffer. This is guaranteed to indicate a complete group if all of our images
// are in chronological order, which is implicit given our time-series in-memory
// storage. This is a very cheap call.
func (bg *bufferedGroup) haveCompleteGroup() bool {
    if len(bg.images) == 0 {
        log.Panicf("a buffered group should never be empty")
    }

    return bg.firstTimeKey != bg.lastTimeKey
}

// havePartialGroup will return true if the group is non-empty but the first
// and last image have the same time-key. This is a very cheap call.
func (bg *bufferedGroup) havePartialGroup() bool {
    if len(bg.images) == 0 {
        log.Panicf("a buffered group should never be empty")
    }

    return bg.firstTimeKey == bg.lastTimeKey
}

// popCompleteGroup is called with the guarantee that we have at least one
// complete group at the head of the buffer, but we need to work our way back
// from the top in order to figure out how many images are actually part of
// that first group. This is where the grouping semantics live.
func (bg *bufferedGroup) popCompleteGroup() (nearestCityKey string, group []*geoindex.GeographicRecord) {
    if bg.haveCompleteGroup() == false {
        log.Panicf("can not return complete group if we do not have one")
    }

    // Iterate through the images at the top of the buffer. Stop when the city
    // or the time-key changes.

    group = make([]*geoindex.GeographicRecord, 0)
    firstNearestCityKey := ""
    firstTimeKey := time.Time{}
    for _, bi := range bg.images {
        if firstNearestCityKey == "" {
            firstNearestCityKey = bi.nearestCityKey
        } else if bi.nearestCityKey != firstNearestCityKey {
            // Break if the current image belongs to a different city than the last.

            break
        }

        if firstTimeKey.IsZero() == true {
            firstTimeKey = bi.effectiveTimekey
        } else if bi.effectiveTimekey != firstTimeKey {
            // Break if the current image belongs to a different time-key than the last.

            break
        }

        group = append(group, bi.gr)
    }

    len_ := len(group)
    if len_ == 0 {
        log.Panicf("the 'first time-key' didn't actually match the first records in the buffer")
    }

    // Prune the front N images.
    bg.images = bg.images[len_:]

    if len(bg.images) == 0 {
        // If we get here, the caller should deallocate us.
        bg.firstTimeKey = time.Time{}
    } else {
        bg.firstTimeKey = bg.images[0].effectiveTimekey
    }

    bg.updateLocationIndex()

    return firstNearestCityKey, group
}

// popPartialGroup returns the tail data from the buffer. These images will all
// be a part of the same group and is partial by the virtue of not being bounded
// but a group following it. This is used as the final step to flush the buffers
// before terminating the search.
func (bg *bufferedGroup) popPartialGroup() (nearestCityKey string, group []*geoindex.GeographicRecord) {
    if bg.haveCompleteGroup() == true {
        log.Panicf("can not return partial group if at least one complete group is available")
    } else if bg.havePartialGroup() == false {
        log.Panicf("can not return partial group if we do not have one")
    }

    group = make([]*geoindex.GeographicRecord, 0)
    nearestCityKey = ""
    for _, bi := range bg.images {
        gr := bi.gr

        if nearestCityKey == "" {
            nearestCityKey = bi.nearestCityKey
        }

        group = append(group, gr)
    }

    // Truncate since we've consumed all contents.
    bg.images = make([]*bufferedImage, 0)

    bg.firstTimeKey = time.Time{}
    bg.lastTimeKey = time.Time{}

    bg.updateLocationIndex()

    return nearestCityKey, group
}

// isEmpty is used to determine when the host `iterativeGroupBuffers` should
// deallocate us.
func (bg *bufferedGroup) isEmpty() bool {
    return len(bg.images) == 0 || bg.firstTimeKey.IsZero()
}

// Push an image into the buffer. Aside from some jitter correction having to do
// with the city this image is associated with compared to the adjacent images,
// this is very straightforward. This is where we might also massage the image
// data in order to facilitate group.
func (bg *bufferedGroup) pushImage(nearestCityKey string, gr *geoindex.GeographicRecord) {
    // If the current image and the last-added image both have the same
    // location, curry that time-key to this image (since they are the same
    // model and location and will now have the same time-key, they'll be
    // grouped together).
    lastBi := bg.images[len(bg.images)-1]

    // Before we push our current image to the back of the buffer, force the
    // time-key of the current image to be inherited from the current-last image
    // (soon to be an adjacent images) if it's the same city.

    var effectiveTimekey time.Time
    if lastBi.nearestCityKey == nearestCityKey {
        effectiveTimekey = bg.lastTimeKey
        gr.AddComment(fmt.Sprintf("Inheriting time-key [%s] of previous record with same city [%s]: [%s] (%.6f, %.6f)", effectiveTimekey, nearestCityKey, path.Base(lastBi.gr.Filepath), lastBi.gr.Latitude, lastBi.gr.Longitude))
    } else {
        gr.AddComment(fmt.Sprintf("Left-adjacent image in buffer is [%s] with different city [%s] at coordinates (%.6f, %.6f) and time-key [%v]", path.Base(lastBi.gr.Filepath), lastBi.nearestCityKey, lastBi.gr.Latitude, lastBi.gr.Longitude, bg.lastTimeKey))
    }

    // Now, append.

    bi := newBufferedImage(nearestCityKey, gr, effectiveTimekey)

    bg.images = append(bg.images, bi)
    currentTimekey := bi.effectiveTimekey

    // Set this before we return in preparation for the next cycle.
    bg.lastTimeKey = currentTimekey

    // This uniquely identifies the current visit to the city. This will help
    // us smooth aberrations in the middle.
    locationTimekey := bi.LocationTimekey()

    len_ := len(bg.images)

    // If our city has already appeared within the current time interval, smooth
    // all of the cities of the images between then and now (which is the last
    // item in the slice) to be the same city. This could easily be caused by
    // just turning around on a walk and/or otherwise backtracking and entering
    // another city near the pivot point within the resolution of the time-key
    // interval.
    if index, found := bg.locationIndex[locationTimekey]; found == true && len_ > 2 {
        firstEncounteredBi := bg.images[index]

        // Sanity check.
        // TODO(dustin): !! Just while debugging.
        if firstEncounteredBi.nearestCityKey != nearestCityKey || firstEncounteredBi.effectiveTimekey != currentTimekey {
            log.Panicf("first encountered index of location-timekey was not recorded right: expected [%s] [%v] rather than [%s] [%v]", nearestCityKey, currentTimekey, firstEncounteredBi.nearestCityKey, firstEncounteredBi.effectiveTimekey)
        }

        // Only update if the item before the item we just added is a different
        // city (but still within the same time-key of our new image. By.
        // Otherwise, we'll just update and reupdate all of the adjacent images
        // that we add that we already know to have the same city.
        previousBi := bg.images[len_-2]
        if previousBi.nearestCityKey != nearestCityKey && previousBi.effectiveTimekey == currentTimekey {
            start_index := index + 1
            n := len(bg.images) - start_index

            for i, bi := range bg.images[start_index:] {
                // Sanity check.
                // TODO(dustin): !! Just while debugging.
                if bi.effectiveTimekey != currentTimekey {
                    log.Panicf("current BI during smoothing is no longer the same time-key: [%v] != [%s]", bi.effectiveTimekey, currentTimekey)
                }

                // The amount of time elapsed between this image and the first
                // image we encountered at the same city and time-key.
                timeSinceAberration := bi.gr.Timestamp.Sub(firstEncounteredBi.gr.Timestamp)

                if bi.nearestCityKey != nearestCityKey {
                    bi.gr.AddComment(fmt.Sprintf("Smoothed image <time-key [%v] timestamp [%v] city [%s] file [%s]> to city [%s] (from just-pushed image <time-key [%v] timestamp [%v] city [%s] file [%s]>). TIME-BETWEEN=[%s] STEP=(%d/%d)", bi.effectiveTimekey, bi.gr.Timestamp, bi.nearestCityKey, path.Base(bi.gr.Filepath), nearestCityKey, currentTimekey, gr.Timestamp, nearestCityKey, path.Base(gr.Filepath), timeSinceAberration, i+1, n))
                    bi.nearestCityKey = nearestCityKey
                }
            }

            bg.updateLocationIndex()
        }
    } else if found == false {
        bg.locationIndex[locationTimekey] = len(bg.images) - 1
    }
}

// updateLocationIndex replaces the current location index with an up-to-date
// one. This is only called if we perform smoothing on the locations on the
// images.
func (bg *bufferedGroup) updateLocationIndex() {
    bg.locationIndex = make(map[string]int)
    for i, bi := range bg.images {
        if _, found := bg.locationIndex[bi.nearestCityKey]; found == false {
            locationTimekey := bi.LocationTimekey()
            bg.locationIndex[locationTimekey] = i
        }
    }
}

func initBufferedGroup(nearestCityKey string, initialGr *geoindex.GeographicRecord) *bufferedGroup {
    initialBi := newBufferedImage(nearestCityKey, initialGr, time.Time{})

    images := []*bufferedImage{
        initialBi,
    }

    return &bufferedGroup{
        firstTimeKey:  initialBi.effectiveTimekey,
        lastTimeKey:   initialBi.effectiveTimekey,
        images:        images,
        locationIndex: make(map[string]int),
    }
}

type iterativeGroupBuffers struct {
    groupsByCameraModel map[string]*bufferedGroup
}

func (igb *iterativeGroupBuffers) dump(printDetail bool) {
    if len(igb.groupsByCameraModel) == 0 {
        fmt.Printf("No images buffered.\n\n")
        return
    }

    for cameraModel, bg := range igb.groupsByCameraModel {
        fmt.Printf("BUFFERED GROUP [%s]\n", cameraModel)
        fmt.Printf("=============================\n")
        fmt.Printf("\n")

        bg.dump(printDetail)
    }
}

func newIterativeGroupBuffers() *iterativeGroupBuffers {
    return &iterativeGroupBuffers{
        groupsByCameraModel: make(map[string]*bufferedGroup),
    }
}

func (igb *iterativeGroupBuffers) bufferedCameraModels() []string {
    models := make([]string, len(igb.groupsByCameraModel))
    i := 0
    for cameraModel, _ := range igb.groupsByCameraModel {
        models[i] = cameraModel
        i++
    }

    return models
}

// haveAnyCompleteGroups returns a model if we have at least one complete group
// in at least one model. This will play a big part in the find-group loop.
func (igb *iterativeGroupBuffers) haveAnyCompleteGroups() string {
    for cameraModel, bg := range igb.groupsByCameraModel {
        if bg.haveCompleteGroup() == true {
            return cameraModel
        }
    }

    return ""
}

// haveAnyPartialGroups returns a model if any of the groups look to wholly
// contain data for just one time-key (the only time we can be sure we have all
// of the images for a group is when werun into a new time-key). We assume we
// are at the end of the index when we finally call this.
func (igb *iterativeGroupBuffers) haveAnyPartialGroups() string {
    for cameraModel, bg := range igb.groupsByCameraModel {
        if bg.havePartialGroup() == true {
            return cameraModel
        }
    }

    return ""
}

// popFirstCompleteGroup will return the first model in the buffer with
// a series of related images that looks like it was followed by an unrelated
// set of images (at a different time, in a different place, or with a different
// camera).
func (igb *iterativeGroupBuffers) popFirstCompleteGroup() (timeKey time.Time, nearestCityKey string, cameraModel string, images []*geoindex.GeographicRecord) {
    electedCameraModel := igb.haveAnyCompleteGroups()
    if electedCameraModel == "" {
        log.Panicf("can not pop a complete group if we do not have one")
    }

    electedBg := igb.groupsByCameraModel[electedCameraModel]
    timeKey = electedBg.firstTimeKey

    nearestCityKey, images = electedBg.popCompleteGroup()
    if electedBg.isEmpty() == true {
        delete(igb.groupsByCameraModel, electedCameraModel)
    }

    return timeKey, nearestCityKey, electedCameraModel, images
}

// popFirstPartialGroup will return the first model with a buffered series of
// related images which must not be followed by another series of images. This
// is a flush operation that will iteratively go from one model to the next,
// clearing what we have once we've exhausted our data source.
func (igb *iterativeGroupBuffers) popFirstPartialGroup() (timeKey time.Time, nearestCityKey string, cameraModel string, images []*geoindex.GeographicRecord) {
    cameraModelWithComplete := igb.haveAnyCompleteGroups()
    if cameraModelWithComplete != "" {
        log.Panicf("can not pop a partial group if we still have complete groups: [%s]", cameraModelWithComplete)
    }

    electedCameraModel := igb.haveAnyPartialGroups()
    if electedCameraModel == "" {
        log.Panicf("can not pop a partial group if we do not have one")
    }

    electedBg := igb.groupsByCameraModel[electedCameraModel]
    timeKey = electedBg.firstTimeKey

    nearestCityKey, images = electedBg.popPartialGroup()
    if electedBg.isEmpty() == false {
        log.Panicf("we expected buffer to be empty after popping a partial group from it: [%s]", electedCameraModel)
    }

    delete(igb.groupsByCameraModel, electedCameraModel)

    return timeKey, nearestCityKey, electedCameraModel, images
}

func (igb *iterativeGroupBuffers) pushImage(nearestCityKey string, gr *geoindex.GeographicRecord) {
    im := gr.Metadata.(geoindex.ImageMetadata)
    cameraModel := im.CameraModel

    if existingGroupBuffer, found := igb.groupsByCameraModel[cameraModel]; found == true {
        existingGroupBuffer.pushImage(nearestCityKey, gr)
    } else {
        igb.groupsByCameraModel[cameraModel] = initBufferedGroup(nearestCityKey, gr)
    }
}
