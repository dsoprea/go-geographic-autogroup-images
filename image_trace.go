package geoautogroup

import (
    "github.com/dsoprea/go-logging"
)

var (
    itLogger = log.NewLogger("geoautogroup.image_trace")

    imageTraceIndex map[string][]string
)

func InitImageTrace(filepaths []string) {
    imageTraceIndex = make(map[string][]string)

    for _, filepath := range filepaths {
        imageTraceIndex[filepath] = nil
    }
}

func PushDebugTrace(filepath, message string) {
    if imageTraceIndex != nil {
        if comments, found := imageTraceIndex[filepath]; found == true {
            imageTraceIndex[filepath] = append(comments, message)
        }
    }
}

func PushWarningTrace(filepath, message string) {
    itLogger.Warningf(nil, message)

    if imageTraceIndex != nil {
        if comments, found := imageTraceIndex[filepath]; found == true {
            imageTraceIndex[filepath] = append(comments, message)
        }
    }
}

func IsImageTraceIndexInited() bool {
    return imageTraceIndex != nil
}

func ImageTraceIndex() map[string][]string {
    return imageTraceIndex
}
