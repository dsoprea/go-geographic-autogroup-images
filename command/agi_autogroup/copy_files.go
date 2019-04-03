package main

import (
    "bytes"
    "fmt"
    "io"
    "os"
    "path"

    "crypto/sha1"
    "text/template"

    "github.com/sbwhitecap/tqdm"
    "github.com/sbwhitecap/tqdm/iterators"

    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"

    "github.com/dsoprea/go-geographic-autogroup-images"
)

func copyFiles(groupArguments groupParameters, fg *geoautogroup.FindGroups, finishedGroupKey geoautogroup.GroupKey, finishedGroup []*geoindex.GeographicRecord, copyRootPath string, imageOutputPathTemplate *template.Template, printProgressOutput bool, binnedImages map[string][]*geoindex.GeographicRecord, fileMappings map[string]imageFileMapping) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    timeKey := finishedGroupKey.TimeKey

    nearestCityIndex := fg.NearestCityIndex()
    cityRecord := nearestCityIndex[finishedGroupKey.NearestCityKey]

    camera_model := finishedGroupKey.CameraModel

    // This will often happen with screen-catpures and pictures downloaded
    // from social networks.
    if camera_model == "" {
        camera_model = "no_camera_model"
    }

    cityprovince := cityRecord.CityAndProvinceState()

    location := cityprovince

    // If the city-record doesn't have a usable province-state string (where
    // `city_and_province_state` equals city), then attach the country name.
    if location == cityRecord.City {
        location = fmt.Sprintf("%s, %s", cityRecord.City, cityRecord.Country)
    }

    // TODO(dustin): !! We should create the directories with the local timezone, not UTC.
    replacements := map[string]interface{}{
        "year":                    timeKey.Year(),
        "month_number":            fmt.Sprintf("%02d", timeKey.Month()),
        "month_name":              fmt.Sprintf("%s", timeKey.Month()),
        "day_number":              fmt.Sprintf("%02d", timeKey.Day()),
        "hour":                    fmt.Sprintf("%02d", timeKey.Hour()),
        "minute":                  fmt.Sprintf("%02d", timeKey.Minute()),
        "second":                  fmt.Sprintf("%02d", timeKey.Second()),
        "city_and_province_state": cityprovince,
        "location":                location,
        "country":                 cityRecord.Country,
        "record_count":            len(finishedGroup),
        "camera_model":            camera_model,
        "path_sep":                string([]byte{os.PathSeparator}),
    }

    b := new(bytes.Buffer)
    err = imageOutputPathTemplate.Execute(b, replacements)
    log.PanicIf(err)

    folderName := b.String()

    destPath := path.Join(copyRootPath, folderName)

    err = os.MkdirAll(destPath, 0755)
    log.PanicIf(err)

    tick := func(gr *geoindex.GeographicRecord) {
        defer func() {
            if state := recover(); state != nil {
                err := log.Wrap(state.(error))
                log.PanicIf(err)
            }
        }()

        if list, found := binnedImages[folderName]; found == true {
            binnedImages[folderName] = append(list, gr)
        } else {
            binnedImages[folderName] = []*geoindex.GeographicRecord{
                gr,
            }
        }

        filename := path.Base(gr.Filepath)

        finalFilename, err := copyFile(groupArguments, destPath, filename, gr, fileMappings)
        log.PanicIf(err)

        destFilepath := path.Join(destPath, finalFilename)
        relFilepathFromCatalog := path.Join("..", "..", folderName, finalFilename)

        fileMappings[gr.Filepath] = imageFileMapping{
            OutputFilepath:              destFilepath,
            RelativeFilepathFromCatalog: relFilepathFromCatalog,
        }
    }

    if printProgressOutput == true {
        // Print the progress of copying all images in this group.

        titleTemplateRaw := "{{.year}}-{{.month_number}}-{{.day_number}} {{.hour}}:{{.minute}}:{{.second}}  {{.location}}{{.path_sep}}{{.camera_model}}"
        titleTemplate := template.Must(template.New("group title template").Parse(titleTemplateRaw))

        b := new(bytes.Buffer)
        err = titleTemplate.Execute(b, replacements)
        log.PanicIf(err)

        title := b.String()

        tqdm.With(iterators.Interval(0, len(finishedGroup)), title, func(v interface{}) (brk bool) {
            defer func() {
                if state := recover(); state != nil {
                    err := log.Wrap(state.(error))
                    log.PanicIf(err)
                }
            }()

            i := v.(int)
            gr := finishedGroup[i]

            tick(gr)

            return false
        })
    } else {
        for _, gr := range finishedGroup {
            tick(gr)
        }
    }

    return nil
}

func copyFile(groupArguments groupParameters, destPath, filename string, gr *geoindex.GeographicRecord, fileMappings map[string]imageFileMapping) (finalFilename string, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    destExt := path.Ext(filename)
    leftSide := filename[:len(filename)-len(destExt)]

    destFilepath := path.Join(destPath, filename)

    // Manage naming collisions.

    // TODO(dustin): Add test.
    for i := 1; i < 10; i++ {
        if f, err := os.Open(destFilepath); err != nil {
            if os.IsNotExist(err) == true {
                break
            }

            log.Panic(err)
        } else {
            f.Close()
        }

        // An optimization.
        if groupArguments.NoHashChecksOnExisting == true {
            return filename, nil
        }

        // File already exists.

        fromImageHash := getFilepathSha1(gr.Filepath)
        ToImageHash := getFilepathSha1(destFilepath)

        // It's identical. Don't do anything.
        if bytes.Compare(fromImageHash, ToImageHash) == 0 {
            mainLogger.Debugf(nil, "Image already exists: [%s] => [%s]", gr.Filepath, destFilepath)
            return filename, nil
        }

        filename = fmt.Sprintf("%s (%d)%s", leftSide, i+1, destExt)
        destFilepath = path.Join(destPath, filename)
    }

    fromFile, err := os.Open(gr.Filepath)
    log.PanicIf(err)

    toFile, err := os.Create(destFilepath)
    log.PanicIf(err)

    _, err = io.Copy(toFile, fromFile)
    log.PanicIf(err)

    fromFile.Close()
    toFile.Close()

    return filename, nil
}

func getFilepathSha1(filepath string) []byte {
    h := sha1.New()

    f, err := os.Open(filepath)
    log.PanicIf(err)

    defer f.Close()

    _, err = io.Copy(h, f)
    log.PanicIf(err)

    hashBytes := h.Sum(nil)[:20]
    return hashBytes
}
