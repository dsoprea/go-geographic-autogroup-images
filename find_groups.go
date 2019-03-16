package geoautogroup

import (
    "errors"
    "fmt"
    "strings"
    "time"

    "github.com/dsoprea/go-logging"

    "github.com/dsoprea/go-geographic-attractor"
    "github.com/dsoprea/go-geographic-attractor/index"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-time-index"
)

var (
    ErrNoMoreGroups         = errors.New("no more groups")
    ErrNoNearLocationRecord = errors.New("no location record was near-enough")
)

const (
    // // DefaultCoalescenceWindowDuration is the distance that we'll use to
    // // determine if the current image might belong to the same group as the last
    // // image if all of the other factors match.
    // DefaultCoalescenceWindowDuration = time.Hour * 24

    // TimeKeyAlignment is a factor that determines how images should be grouped
    // together on the basis of their timestamps if their grouping factors are
    // otherwise identical. In seconds.
    TimeKeyAlignment = 60 * 10
)

const (
    SkipReasonNoNearLocationRecord = "no matching/near location record"
    SkipReasonNoNearCity           = "no near city"
)

const (
    LocationMatchStrategyBestGuess  = "best guess"
    LocationMatchStrategySparseData = "sparse data"
)

var (
    findGroupsLogger = log.NewLogger("geogroup.find_groups")
)

type UnassignedRecord struct {
    Geographic *geoindex.GeographicRecord
    Reason     string
}

type GroupKey struct {
    TimeKey        time.Time `json:"time_key"`
    NearestCityKey string    `json:"nearest_city_key"`
    CameraModel    string    `json:"camera_model"`
}

func (gk GroupKey) String() string {
    textBytes, err := gk.TimeKey.MarshalText()
    log.PanicIf(err)

    return fmt.Sprintf("GroupKey<TIME-KEY=[%s] NEAREST-CITY=[%s] CAMERA-MODEL=[%s]>", string(textBytes), gk.NearestCityKey, gk.CameraModel)
}

func (gk GroupKey) KeyPhrase() string {
    timestampPhrase := gk.TimeKey.Format(time.RFC3339)
    timestampPhrase = strings.Replace(timestampPhrase, ":", "-", -1)

    return fmt.Sprintf("%s-%s-%s", timestampPhrase, gk.NearestCityKey, gk.CameraModel)
}

type FindGroups struct {
    locationTs           timeindex.TimeSlice
    imageTs              timeindex.TimeSlice
    unassignedRecords    []UnassignedRecord
    currentImagePosition int
    cityIndex            *geoattractorindex.CityIndex
    nearestCityIndex     map[string]geoattractor.CityRecord
    currentGroupKey      map[string]GroupKey
    currentGroup         map[string][]*geoindex.GeographicRecord

    // roundingWindowDuration    time.Duration
    // coalescenceWindowDuration time.Duration

    locationMatcherFn LocationMatcherFn
}

type LocationMatcherFn func(imageTe timeindex.TimeEntry) (matchedTe timeindex.TimeEntry, err error)

func NewFindGroups(locationTs timeindex.TimeSlice, imageTs timeindex.TimeSlice, ci *geoattractorindex.CityIndex) *FindGroups {
    if len(locationTs) == 0 {
        log.Panicf("no locations")
    }

    fg := &FindGroups{
        locationTs:        locationTs,
        imageTs:           imageTs,
        unassignedRecords: make([]UnassignedRecord, 0),
        cityIndex:         ci,
        nearestCityIndex:  make(map[string]geoattractor.CityRecord),
        currentGroupKey:   make(map[string]GroupKey),
        currentGroup:      make(map[string][]*geoindex.GeographicRecord, 0),
        // roundingWindowDuration:    DefaultRoundingWindowDuration,
        // coalescenceWindowDuration: DefaultCoalescenceWindowDuration,
    }

    fg.locationMatcherFn = fg.findLocationByTimeBestGuess

    return fg
}

func (fg *FindGroups) SetLocationMatchStrategy(strategy string) {
    if strategy == LocationMatchStrategySparseData {
        fg.locationMatcherFn = fg.findLocationByTimeWithSparseLocations
    } else if strategy == LocationMatchStrategyBestGuess {
        fg.locationMatcherFn = fg.findLocationByTimeBestGuess
    } else {
        log.Panicf("location-match strategy [%s] not valid", strategy)
    }
}

// func (fg *FindGroups) SetRoundingWindowDuration(roundingWindowDuration time.Duration) {
//     fg.roundingWindowDuration = roundingWindowDuration
// }

// func (fg *FindGroups) SetCoalescenceWindowDuration(coalescenceWindowDuration time.Duration) {
//     fg.coalescenceWindowDuration = coalescenceWindowDuration
// }

// NearestCityIndex returns all of the cities that we've grouped the images by
// in a map keyed the same as in the grouping.
func (fg *FindGroups) NearestCityIndex() map[string]geoattractor.CityRecord {
    return fg.nearestCityIndex
}

func (fg *FindGroups) UnassignedRecords() []UnassignedRecord {
    return fg.unassignedRecords
}

func (fg *FindGroups) addUnassigned(gr *geoindex.GeographicRecord, reason string) {
    ur := UnassignedRecord{
        Geographic: gr,
        Reason:     reason,
    }

    fg.unassignedRecords = append(fg.unassignedRecords, ur)

    findGroupsLogger.Warningf(nil, "Skipping %s: %s", gr, reason)
}

// findLocationByTime returns the nearest location record to the timestamp in
// the given image record.
//
// Note that we keep separate bins for separate camera models. This mitigates
// producing a bunch of fragmented groups if someone combined pictures from
// multiple people or multiple cameras.
func (fg *FindGroups) findLocationByTimeBestGuess(imageTe timeindex.TimeEntry) (matchedTe timeindex.TimeEntry, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // DefaultRoundingWindowDuration is the largest time duration we're allowed
    // to search for matching location records within for a given image.
    roundingWindowDuration := time.Minute * 10

    locationIndexTs := fg.locationTs

    // nearestLocationPosition is either the position where the exact
    // time of the image was found in the location index or the
    // position that it would be inserted (even though we're not
    // interested in insertions).
    //
    // Both the location and image indices are ordered, obviously;
    // technically we could potentially read along both and avoid a
    // bunch of bunch searches. However, the location index will be
    // frequented by large gaps that have no corresponding images and
    // we're just going to end-up seeking more that way.
    nearestLocationPosition := timeindex.SearchTimes(locationIndexTs, imageTe.Time)

    var previousLocationTe timeindex.TimeEntry
    var nextLocationTe timeindex.TimeEntry

    if nearestLocationPosition >= len(locationIndexTs) {
        // We were given a position past the end of the list.

        previousLocationTe = locationIndexTs[len(locationIndexTs)-1]
    } else {
        // We were given a position within the list.

        nearestLocationTe := locationIndexTs[nearestLocationPosition]
        if nearestLocationTe.Time == imageTe.Time {
            // We found a location record that exactly matched our
            // image record (time-wise).

            return nearestLocationTe, nil
        } else {
            // This is an optimistic insertion-position recommendation
            // (`nearestLocationPosition` is a existing record that is
            // larger than our query).

            nextLocationTe = nearestLocationTe
        }

        // If there's at least one more entry to the left,
        // calculate the distance to it.
        if nearestLocationPosition > 0 {
            previousLocationTe = locationIndexTs[nearestLocationPosition-1]
        }
    }

    var durationSincePrevious time.Duration
    if previousLocationTe.IsZero() == false {
        durationSincePrevious = imageTe.Time.Sub(previousLocationTe.Time)
    }

    var durationUntilNext time.Duration
    if nextLocationTe.IsZero() == false {
        durationUntilNext = nextLocationTe.Time.Sub(imageTe.Time)
    }

    if durationSincePrevious != 0 {
        if durationSincePrevious <= roundingWindowDuration && (durationUntilNext == 0 || durationUntilNext > roundingWindowDuration) {
            // Only the preceding time duration is acceptable.
            matchedTe = previousLocationTe
        } else if durationSincePrevious <= roundingWindowDuration && durationUntilNext != 0 && durationUntilNext <= roundingWindowDuration {
            // They're both fine. Take the nearest.

            if durationSincePrevious < durationUntilNext {
                matchedTe = previousLocationTe
            } else {
                matchedTe = nextLocationTe
            }
        }
    }

    // Effectively, the "else" for the above.
    if durationUntilNext != 0 && matchedTe.IsZero() == true && durationUntilNext < roundingWindowDuration {
        matchedTe = nextLocationTe
    }

    if matchedTe.Time.IsZero() == true {
        return timeindex.TimeEntry{}, ErrNoNearLocationRecord
    }

    return matchedTe, nil
}

// findLocationByTimeWithSparseLocations uses the last location recorded within
// the last twelve hours. This is for use with high-confidence datasets that are
// recording continuously unless the subject/device has remained stationary
// (which would minimize duplicate points).
func (fg *FindGroups) findLocationByTimeWithSparseLocations(imageTe timeindex.TimeEntry) (matchedTe timeindex.TimeEntry, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    locationIndexTs := fg.locationTs

    // nearestLocationPosition is either the position where the exact
    // time of the image was found in the location index or the
    // position that it would be inserted (even though we're not
    // interested in insertions).
    //
    // Both the location and image indices are ordered, obviously;
    // technically we could potentially read along both and avoid a
    // bunch of bunch searches. However, the location index will be
    // frequented by large gaps that have no corresponding images and
    // we're just going to end-up seeking more that way.
    nearestLocationPosition := timeindex.SearchTimes(locationIndexTs, imageTe.Time)

    maxProximityDuration := time.Hour * 12

    if nearestLocationPosition >= len(locationIndexTs) {
        // We were given a position past the end of the list.

        lastTe := locationIndexTs[len(locationIndexTs)-1]
        if imageTe.Time.Sub(lastTe.Time) <= maxProximityDuration {
            // The last item in the list is still within proximity.

            return lastTe, nil
        }

        // No match.
        return timeindex.TimeEntry{}, ErrNoNearLocationRecord
    }

    // We were given a position within the list.

    nearestLocationTe := locationIndexTs[nearestLocationPosition]
    if nearestLocationTe.Time == imageTe.Time {
        // We found a location record that exactly matched our
        // image record (time-wise).

        return nearestLocationTe, nil
    }

    // We found a location record with a time larger than our image's
    // time.

    if nearestLocationPosition > 0 {
        // There was a record before this (with a timestamp that
        // necessarily be lower) one so we'll take that instead.

        matchedTe = locationIndexTs[nearestLocationPosition-1]
        return matchedTe, nil
    } else if nearestLocationTe.Time.Sub(imageTe.Time) <= maxProximityDuration {
        // This is the first record we have (the image's timestamp must
        // be earlier than the data we have). However, our image's
        // timestamp still occurs within proximity.

        return nearestLocationTe, nil
    }

    // No match.
    return timeindex.TimeEntry{}, ErrNoNearLocationRecord
}

// flushCurrentGroup will capture the current set of grouped images, truncate
// the list, set the next group key as the current group key, and return. Note
// that this only acts on the current group of the same camera-model as the next
// group-key.
func (fg *FindGroups) flushCurrentGroup(nextGroupKey GroupKey) (finishedGroupKey GroupKey, finishedGroup []*geoindex.GeographicRecord, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    cameraModel := nextGroupKey.CameraModel

    // Note that this will be nil if we don't currently have anything buffered.
    finishedGroup = fg.currentGroup[cameraModel]

    delete(fg.currentGroup, cameraModel)

    finishedGroupKey = fg.currentGroupKey[cameraModel]
    fg.currentGroupKey[cameraModel] = nextGroupKey

    return finishedGroupKey, finishedGroup, nil
}

// getAlignedEpoch returns an aligned epoch time. Used to determine grouping.
func getAlignedEpoch(epoch int64) int64 {
    return epoch - epoch%TimeKeyAlignment
}

func getAlignedTime(t time.Time) time.Time {
    epoch := t.Unix()
    epoch = getAlignedEpoch(epoch)

    return time.Unix(epoch, 0).UTC()
}

// FindNext returns the next set of grouped-images along with the actual
// grouping factors.
//
//
// BIG FAT NOTE ON ORDERING
// ==
//
// We internally enumerate the previously-loaded time-ordered images and store
// them into a hash, keyed by camera-model. The camera-model-based storage is
// there to prevent multiple sets of overlapping images from interfering with
// how we group images. As a result, when the camera-model changes from one
// image to the next, the images previously grouped for a given model will stay
// in the buffer until the very end until we've seen all images and begin to
// flush the buffered groups of images. *At this point*, which groups of
// buffered images will be returned first will depend on Go's hash algorithm.
// Whichever model is visited in the `currentGroup`/`currentGroupKey` hashes
// first on every call to this function will determine that.
//
// Note that the above ordering behavior only applies when only the model
// changes from one image to the next. If other grouping factors change but the
// model stays the same, the images already collected for that model will be
// returned immediately.
func (fg *FindGroups) FindNext() (finishedGroupKey GroupKey, finishedGroup []*geoindex.GeographicRecord, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    imageIndexTs := fg.imageTs

    if fg.currentImagePosition >= len(imageIndexTs) {
        // Do one iteration, at most, just to pop exactly one group out of
        // the buffer if we have anything.
        //
        // We use `fg.currentGroup` rather than `fg.currentGroupKey`, directly,
        // because `flushCurrentGroup()` will always leave at least one item in
        // `fg.currentGroupKey`.
        for cameraModel, _ := range fg.currentGroup {
            currentGroupKey := fg.currentGroupKey[cameraModel]

            finishedGroupKey, finishedGroup, err = fg.flushCurrentGroup(currentGroupKey)
            log.PanicIf(err)

            if finishedGroup == nil {
                log.Panicf("there are no grouped images as expected (1)")
            }

            return finishedGroupKey, finishedGroup, nil
        }

        return GroupKey{}, nil, ErrNoMoreGroups
    }

    // Loop through all timestamps starting from where we left off.
    for ; fg.currentImagePosition < len(imageIndexTs); fg.currentImagePosition++ {
        imageTe := imageIndexTs[fg.currentImagePosition]
        for _, item := range imageTe.Items {
            // TODO(dustin): !! We'll skip images when we encounter a new group and return the existing one, if there are additional images on this timestamps. We should just set a flag and then terminate after we finish processing these items.

            imageGr := item.(*geoindex.GeographicRecord)

            if imageGr.HasGeographic == false {
                matchedTe, err := fg.locationMatcherFn(imageTe)
                if err != nil {
                    if log.Is(err, ErrNoNearLocationRecord) == true {
                        fg.addUnassigned(imageGr, SkipReasonNoNearLocationRecord)
                        continue
                    }

                    log.Panic(err)
                }

                locationItem := matchedTe.Items[0]
                locationGr := locationItem.(*geoindex.GeographicRecord)

                // The location index should exclusively be loaded with
                // geographic data. This should never happen.
                if locationGr.HasGeographic == false {
                    log.Panicf("location record indicates no geographic data; this should never happen")
                }

                imageGr.Latitude = locationGr.Latitude
                imageGr.Longitude = locationGr.Longitude
                imageGr.S2CellId = locationGr.S2CellId
            }

            // If we got here, we either have or have found a location for the
            // given image.

            im := imageGr.Metadata.(geoindex.ImageMetadata)
            cameraModel := im.CameraModel

            // Now, we'll construct the group that this image should be a part
            // of. Later, we'll compare the groups of each image to the groups
            // of adjacent images in order to determine which should be binned
            // together.

            // First, find a city to associate this location with.

            sourceName, _, cr, err := fg.cityIndex.Nearest(imageGr.Latitude, imageGr.Longitude)
            if err != nil {
                if log.Is(err, geoattractorindex.ErrNoNearestCity) == true {
                    fg.addUnassigned(imageGr, SkipReasonNoNearCity)
                    continue
                }

                log.Panic(err)
            }

            nearestCityKey := fmt.Sprintf("%s,%s", sourceName, cr.Id)
            fg.nearestCityIndex[nearestCityKey] = cr

            // Determine what timestamp to associate this image to. The time-
            // key is the image's time rounded down to a ten-minute alignment.

            imageUnixTime := imageTe.Time.Unix()
            normalImageUnixTime := getAlignedEpoch(imageUnixTime)

            timeKey := time.Unix(normalImageUnixTime, 0).UTC()

            currentGroupKey := fg.currentGroupKey[cameraModel]
            currentGroupKeyTimeKey := currentGroupKey.TimeKey
            if currentGroupKeyTimeKey.IsZero() == false {
                // We're already tracking a group for this camera model.

                // Check our sanity.
                if currentGroupKey.CameraModel != cameraModel {
                    log.Panicf("currently tracked camera-model does not equal current image camera-model where we are")
                }

                if currentGroupKey.NearestCityKey == nearestCityKey {
                    // If the group we're currently tracking is the same city,
                    // reuse the time-key. This means that adjacent groups will
                    // always be merged if the only difference is time.

                    timeKey = currentGroupKeyTimeKey
                }

                // Given the canges above, if the last group's other factors
                // also match the factors we're going to assemble below, this
                // group-key will end up being the same.
            }

            // Build the group key.

            gk := GroupKey{
                TimeKey:        timeKey,
                NearestCityKey: nearestCityKey,
                CameraModel:    cameraModel,
            }

            currentGroupKey, currentGroupKeyFound := fg.currentGroupKey[cameraModel]
            if currentGroupKeyFound == false {
                fg.currentGroupKey[cameraModel] = gk

                fg.currentGroup[cameraModel] = []*geoindex.GeographicRecord{
                    imageGr,
                }
            } else if gk != currentGroupKey {
                finishedGroupKey, finishedGroup, err = fg.flushCurrentGroup(gk)
                log.PanicIf(err)

                // The "current" group-key has been update but we still have to
                // push our current image into the buffer.

                if existingGroup, found := fg.currentGroup[cameraModel]; found == true {
                    fg.currentGroup[cameraModel] = append(existingGroup, imageGr)
                } else {
                    fg.currentGroup[cameraModel] = []*geoindex.GeographicRecord{
                        imageGr,
                    }
                }

                if finishedGroup != nil {
                    fg.currentImagePosition++

                    return finishedGroupKey, finishedGroup, nil
                }
            } else {
                if existingGroup, found := fg.currentGroup[cameraModel]; found == true {
                    fg.currentGroup[cameraModel] = append(existingGroup, imageGr)
                } else {
                    fg.currentGroup[cameraModel] = []*geoindex.GeographicRecord{
                        imageGr,
                    }
                }
            }
        }
    }

    // Do one iteration, at most, just to pop exactly one group out of
    // the buffer if we have anything.
    //
    // We use `fg.currentGroup` rather than `fg.currentGroupKey`, directly,
    // because `flushCurrentGroup()` will always leave at least one item in
    // `fg.currentGroupKey`.
    for cameraModel, _ := range fg.currentGroup {
        currentGroupKey := fg.currentGroupKey[cameraModel]

        finishedGroupKey, finishedGroup, err = fg.flushCurrentGroup(currentGroupKey)
        log.PanicIf(err)

        // TODO(dustin): !! flushCurrentGroup() can return `nil` for `finishedGroup`. Do we need to check/assert that, here?

        return finishedGroupKey, finishedGroup, nil
    }

    return GroupKey{}, nil, ErrNoMoreGroups
}
