package geoautogroup

import (
    "errors"
    "fmt"
    "path"
    "strings"
    "time"

    "github.com/dsoprea/go-logging"
    "github.com/golang/geo/s2"

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

    bufferedGroups *iterativeGroupBuffers
}

type LocationMatcherFn func(imageTe timeindex.TimeEntry) (matchedTe timeindex.TimeEntry, err error)

func NewFindGroups(locationTs timeindex.TimeSlice, imageTs timeindex.TimeSlice, ci *geoattractorindex.CityIndex) *FindGroups {
    if len(locationTs) == 0 {
        log.Panicf("no locations")
    }

    igb := newIterativeGroupBuffers()

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
        bufferedGroups: igb,
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

// getAlignedEpoch returns an aligned epoch time. Used to determine grouping.
func getAlignedEpoch(epoch int64) int64 {
    return epoch - epoch%TimeKeyAlignment
}

func getAlignedTime(t time.Time) time.Time {
    epoch := t.Unix()
    epoch = getAlignedEpoch(epoch)

    return time.Unix(epoch, 0).UTC()
}

type currentImageRecord struct {
    ImageUnixTime    time.Time
    GeographicRecord *geoindex.GeographicRecord
    NearestCityKey   string
}

// getCurrentPositionImages returns the images as the current position in the
// image time-series index.
func (fg *FindGroups) getCurrentPositionImages() (outputRecords []currentImageRecord, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    imageTe := fg.imageTs[fg.currentImagePosition]
    outputRecords = make([]currentImageRecord, 0)
    for _, item := range imageTe.Items {
        // TODO(dustin): !! We'll skip images when we encounter a new group and return the existing one, if there are additional images on this timestamps. We should just set a flag and then terminate after we finish processing these items.

        imageGr := item.(*geoindex.GeographicRecord)

        if imageGr.HasGeographic == false {
            // TODO(dustin): Note that we match for a location based on the timestamp in the index but that we group based on the timestamp in the image. The original was an earlier design but going with the last will likely always be at least identical accuracy and the design is a little more intuitive. Refactor the location-matching to use the image time.
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

            cell := s2.CellID(locationGr.S2CellId)

            comment := fmt.Sprintf("Updated geographic from location with filename [%s], timestamp [%s], and cell [%s]", path.Base(locationGr.Filepath), locationGr.Timestamp.Format(time.RFC3339), cell.ToToken())
            imageGr.AddComment(comment)
        }

        // Now, we'll construct the group that this image should be a part
        // of. Later, we'll compare the groups of each image to the groups
        // of adjacent images in order to determine which should be binned
        // together.

        // First, find a city to associate this location with.

        // TODO(dustin): !! We already have a cell-ID in `imageGr`. Use that directly rather than forcing downstream recalculations of it by not passing it?
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

        cir := currentImageRecord{
            ImageUnixTime:    imageTe.Time,
            GeographicRecord: imageGr,
            NearestCityKey:   nearestCityKey,
        }

        outputRecords = append(outputRecords, cir)
    }

    return outputRecords, nil
}

func getGeographicRecordTimeKey(gr *geoindex.GeographicRecord) time.Time {
    imageUnixTime := gr.Timestamp.Unix()
    normalImageUnixTime := getAlignedEpoch(imageUnixTime)

    timeKey := time.Unix(normalImageUnixTime, 0).UTC()
    return timeKey
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

    // Try popping immediately.

    if fg.bufferedGroups.haveAnyCompleteGroups() != "" {
        timeKey, nearestCityKey, cameraModel, images := fg.bufferedGroups.popFirstCompleteGroup()

        gk := GroupKey{
            TimeKey:        timeKey,
            NearestCityKey: nearestCityKey,
            CameraModel:    cameraModel,
        }

        return gk, images, nil
    }

    // If we get here, no data. Try loading.

    imageIndexTs := fg.imageTs

    // Loop through all timestamps starting from where we left off.
    for ; fg.currentImagePosition < len(imageIndexTs); fg.currentImagePosition++ {
        currentImageRecords, err := fg.getCurrentPositionImages()
        log.PanicIf(err)

        for _, cir := range currentImageRecords {
            imageGr := cir.GeographicRecord
            nearestCityKey := cir.NearestCityKey

            fg.bufferedGroups.pushImage(nearestCityKey, imageGr)
        }
    }

    if fg.bufferedGroups.haveAnyCompleteGroups() != "" {
        timeKey, nearestCityKey, cameraModel, images := fg.bufferedGroups.popFirstCompleteGroup()

        gk := GroupKey{
            TimeKey:        timeKey,
            NearestCityKey: nearestCityKey,
            CameraModel:    cameraModel,
        }

        return gk, images, nil
    }

    // If we get here, we're out of data. Is there data in the buffer?

    if fg.bufferedGroups.haveAnyPartialGroups() != "" {
        timeKey, nearestCityKey, cameraModel, images := fg.bufferedGroups.popFirstPartialGroup()

        gk := GroupKey{
            TimeKey:        timeKey,
            NearestCityKey: nearestCityKey,
            CameraModel:    cameraModel,
        }

        return gk, images, nil
    }

    // This should never happen.
    if fg.currentImagePosition < len(imageIndexTs) {
        log.Panicf("no data in buffer but we still appear to have data in the data-source")
    }

    return GroupKey{}, nil, ErrNoMoreGroups
}
