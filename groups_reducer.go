package geoautogroup

import (
    "fmt"

    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
)

const (
    trivialGroupMaximumSize = 20
)

type GroupsReducer struct {
    fg *FindGroups
}

func NewGroupsReducer(fg *FindGroups) *GroupsReducer {
    return &GroupsReducer{
        fg: fg,
    }
}

type collectedGroup struct {
    GroupKey GroupKey
    Records  []*geoindex.GeographicRecord
}

// Reduce simultaneously iterates through the group process and performs a
// secondary analysis on the output groups to see if any are so small that
// they can just be merged to the last on the same day. This works because
// we get the images in chronological order.
func (gr *GroupsReducer) Reduce() (finishedGroups map[string][]*collectedGroup, merged int) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    finishedGroups = make(map[string][]*collectedGroup)
    lastGroup := make(map[string]*collectedGroup)

    for {
        groupKey, records, err := gr.fg.FindNext()
        if err != nil {
            if err == ErrNoMoreGroups {
                break
            }

            log.Panic(err)
        }

        lastCg, found := lastGroup[groupKey.CameraModel]
        if found == false {
            // We aren't yet tracking anything for the current model.

            lastGroup[groupKey.CameraModel] = &collectedGroup{
                GroupKey: groupKey,
                Records:  records,
            }

            continue
        }

        lastCameraModel := lastCg.GroupKey.CameraModel

        // We have one in the hopper. Can we merge?

        // TODO(dustin): !! We should create the directories with the local timezone, not UTC.
        // TODO(dustin): !! This comparison needs to convert to the local timezone first.
        isDifferentDay := lastCg.GroupKey.TimeKey.Year() != groupKey.TimeKey.Year() || lastCg.GroupKey.TimeKey.Month() != groupKey.TimeKey.Month() || lastCg.GroupKey.TimeKey.Day() != groupKey.TimeKey.Day()
        lastWasLarge := len(lastCg.Records) > trivialGroupMaximumSize
        currentIsLarge := len(records) > trivialGroupMaximumSize
        if isDifferentDay || lastWasLarge && currentIsLarge {
            // Either the current and the last group are not trivial or on
            // different days. Don't merge. Start tracking the new group and
            // return the last one.

            if finishedModelGroups, found := finishedGroups[lastCameraModel]; found == true {
                finishedGroups[lastCameraModel] = append(finishedModelGroups, lastCg)
            } else {
                finishedGroups[lastCameraModel] = []*collectedGroup{lastCg}
            }

            lastGroup[groupKey.CameraModel] = &collectedGroup{
                GroupKey: groupKey,
                Records:  records,
            }

            continue
        }

        // If we get here, we have a green-light to go forward with the merge.

        if lastWasLarge == true {
            // If the current group is trivial but the last wasn't.

            originalLen := len(lastCg.Records)
            lastCg.Records = append(lastCg.Records, records...)

            // Add a comment to each of these images.

            comment := fmt.Sprintf("Appended to a larger group when dropping trivial group: %s (%d) => %s (%d)", groupKey, len(records), lastCg.GroupKey, originalLen)
            for _, gr := range records {
                gr.AddComment(comment)
            }
        } else {
            // If the current group is trivial, regardless of how big the last one was. Either way, we're merging.

            toPrepend := lastCg.Records[:]
            originalLen := len(records)
            records = append(toPrepend, records...)

            // Add a comment to each of these images.

            comment := fmt.Sprintf("Prepended to a larger group when dropping trivial group: %s (%d) => %s (%d)", lastCg.GroupKey, len(lastCg.Records), groupKey, originalLen)
            for _, gr := range lastCg.Records {
                gr.AddComment(comment)
            }

            lastCg.GroupKey = groupKey
            lastCg.Records = records
        }

        merged++
    }

    // Flush.

    for cameraModel, lastCg := range lastGroup {
        if finishedModelGroups, found := finishedGroups[cameraModel]; found == true {
            finishedGroups[cameraModel] = append(finishedModelGroups, lastCg)
        } else {
            finishedGroups[cameraModel] = []*collectedGroup{lastCg}
        }
    }

    return finishedGroups, merged
}
