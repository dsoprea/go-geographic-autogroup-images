[![Build Status](https://travis-ci.org/dsoprea/go-geographic-autogroup-images.svg?branch=master)](https://travis-ci.org/dsoprea/go-geographic-autogroup-images)
[![Coverage Status](https://coveralls.io/repos/github/dsoprea/go-geographic-autogroup-images/badge.svg?branch=master)](https://coveralls.io/github/dsoprea/go-geographic-autogroup-images?branch=master)
[![GoDoc](https://godoc.org/github.com/dsoprea/go-geographic-autogroup-images?status.svg)](https://godoc.org/github.com/dsoprea/go-geographic-autogroup-images)


# Overview

Given loaded location and image indices, iteratively identify geographical groupings of images. Since the images are stored in a time-series in the index, this will effectively group pictures by visit to geographical place.


# Features

- Image coordinates are taken directly from images if present. Otherwise, the images will be matched against the location index in order to determine the approximate area where the image was taken.
- The factors used to approximate how we match images to locations and whether we assign an image to the same group as earlier images versus a new group are documented here: [Constants](https://godoc.org/github.com/dsoprea/go-geographic-autogroup-images#pkg-constants).
- Images are grouped based on timestamps, urban areas, and camera model.
- Images that do not have a location and can not be assigned a location based on the location index will be logged and skipped. See [FindGroups.UnassignedRecords](https://godoc.org/github.com/dsoprea/go-geographic-autogroup-images#FindGroups.UnassignedRecords).


# Components

- [github.com/dsoprea/go-geographic-index](https://github.com/dsoprea/go-geographic-index)
- [github.com/dsoprea/go-geographic-attractor](https://github.com/dsoprea/go-geographic-attractor)


# Example

Excerpt from [FindGroups.FindNext](https://godoc.org/github.com/dsoprea/go-geographic-autogroup-images#example-FindGroups-FindNext) example:

```go
fg := NewFindGroups(locationIndex, imageIndex, cityIndex)

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
```


# Notes

## Camera Model

The camera model is extracted from EXIF (or interpreted as an empty-string if none). Grouping by camera model is important because there frequently might be images from other people within the same search space as your own images, where both have overlapping timeframes. The implementation also prevents confusion in how to define groups when adjacent images are identified with locations in different parts of the world. There is no normal use-case where this behavior would make sense among images from the same camera.

*This functionality has limited usefulness if your friends are using the same device as you and are sharing images directly. The harm from the former is mitigated when using social networks since they will usually strip EXIF information, therefore giving them an effective camera-model of "" (empty string). This will obviously be different from the camera-model that will be read directly from your personal images, therefore enabling us to maintain the separation.*
