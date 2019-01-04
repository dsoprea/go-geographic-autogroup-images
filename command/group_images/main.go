package main

import (
    "bytes"
    "encoding/json"
    "encoding/xml"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "path"
    "sort"
    "text/template"
    "time"

    "github.com/dsoprea/go-geographic-attractor"
    "github.com/dsoprea/go-geographic-autogroup-images"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
    "github.com/jessevdk/go-flags"
    "github.com/twpayne/go-kml"
)

var (
    mainLogger = log.NewLogger("main")
)

const (
    copyInfoFilenamePrefix = ".autogroup"
)

type tallyItem struct {
    name  string
    count int
}

type Tallies []tallyItem

// Len is the number of elements in the collection.
func (tallies Tallies) Len() int {
    return len(tallies)
}

// Less sorts in reverse so that a call to sort.Sort() will produce a
// descendingly-ordered list.
func (tallies Tallies) Less(i, j int) bool {
    return tallies[i].count > tallies[j].count
}

func (tallies Tallies) Swap(i, j int) {
    tallies[i], tallies[j] = tallies[j], tallies[i]
}

// attractorParameters are the parameters common to anything that needs to load
// a `geoattractorindex.CityIndex`.
type attractorParameters struct {
    CountriesFilepath string `long:"countries-filepath" description:"File-path of the GeoNames countries data (usually called 'countryInfo.txt')" required:"true"`
    CitiesFilepath    string `long:"cities-filepath" description:"File-path of the GeoNames world-cities data (usually called 'allCountries.txt')" required:"true"`
}

// indexParameters are the parameters common to anything that needs to load a
// `geoindex.GeographicCollector`.
type indexParameters struct {
    DataPaths  []string `long:"data-path" description:"Path to scan for geographic data (GPX files and image files; can be provided more than once)" required:"true"`
    ImagePaths []string `long:"image-path" description:"Path to scan for images to group (can be provided more than once)" required:"true"`
}

type groupParameters struct {
    attractorParameters
    indexParameters

    LocationsAreSparse        bool   `long:"sparse-data" description:"Location data is sparse. Sparse datasets will not record points if there has been no movement."`
    KmlFilepath               string `long:"kml-filepath" description:"Write KML to the given file"`
    KmlMinimumGroupImageCount int    `long:"kml-minimum" description:"Exclude groups with less than N images from the KML" default:"20"`
    JsonFilepath              string `long:"json-filepath" description:"Write JSON to the given file"`
    UnassignedFilepath        string `long:"unassigned-filepath" description:"File to write unassigned files to"`
    PrintStats                bool   `long:"stats" description:"Print statistics"`
    CopyPath                  string `long:"copy-into-path" description:"Copy grouped images into this path."`
    ImageOutputPathTemplate   string `long:"output-template" description:"Group output path name template within the output path. Can use Go template tokens." default:"{{.year}}-{{.month_number}} {{.location}}{{.path_sep}}{{.year}}-{{.month_number}}-{{.day_number}}{{.path_sep}}{{.camera_model}}"`
    NoPrintDotOutput          bool   `long:"no-dots" description:"Don't print dot progress output if copying"`
}

type subcommands struct {
    Group groupParameters `command:"group" description:"Grouping operations"`
}

var (
    rootArguments = new(subcommands)
)

func getFindGroups(groupArguments groupParameters) (fg *geoautogroup.FindGroups) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    ci, err := geoautogroup.GetCityIndex(groupArguments.attractorParameters.CountriesFilepath, groupArguments.attractorParameters.CitiesFilepath)
    log.PanicIf(err)

    if groupArguments.PrintStats == true {
        fmt.Printf("Attractor index stats: %s\n", ci.Stats())
    }

    locationIndex, err := geoautogroup.GetGeographicIndex(groupArguments.indexParameters.DataPaths)
    log.PanicIf(err)

    if groupArguments.PrintStats == true {
        fmt.Printf("(%d) records loaded in location index.\n", len(locationIndex.Series()))
    }

    imageIndex, err := geoautogroup.GetGeographicIndex(groupArguments.indexParameters.ImagePaths)
    log.PanicIf(err)

    if groupArguments.PrintStats == true {
        fmt.Printf("(%d) records loaded in image index.\n", len(imageIndex.Series()))
    }

    fg = geoautogroup.NewFindGroups(locationIndex, imageIndex, ci)

    if groupArguments.LocationsAreSparse == true {
        fg.SetLocationMatchStrategy(geoautogroup.LocationMatchStrategySparseData)
    }

    return fg
}

func handleGroup(groupArguments groupParameters) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    fg := getFindGroups(groupArguments)

    kmlTallies := make(map[geoattractor.CityRecord][2]int)

    var collected []interface{}
    if groupArguments.JsonFilepath != "" {
        collected = make([]interface{}, 0)
    }

    imageOutputPathTemplate := template.Must(template.New("image output template").Parse(groupArguments.ImageOutputPathTemplate))

    // We use a map to ensure uniqueness.
    destPaths := make(map[string]int)

    printDotOutput := (groupArguments.NoPrintDotOutput == false)
    for i := 0; ; i++ {
        finishedGroupKey, finishedGroup, err := fg.FindNext()
        if err != nil {
            if err == geoautogroup.ErrNoMoreGroups {
                break
            }

            log.Panic(err)
        }

        if collected != nil {
            item := map[string]interface{}{
                "group_key": finishedGroupKey,
                "records":   finishedGroup,
            }

            collected = append(collected, item)
        }

        if groupArguments.CopyPath != "" {
            destPath, err := copyFile(fg, finishedGroupKey, finishedGroup, groupArguments.CopyPath, imageOutputPathTemplate, printDotOutput)
            log.PanicIf(err)

            destPaths[destPath] = len(finishedGroup)
        }

        // TODO(dustin): Just to get rid of incidental pictures from the journey.
        if len(finishedGroup) < groupArguments.KmlMinimumGroupImageCount {
            continue
        }

        nearestCityIndex := fg.NearestCityIndex()
        cityRecord := nearestCityIndex[finishedGroupKey.NearestCityKey]

        if existing, found := kmlTallies[cityRecord]; found == true {
            kmlTallies[cityRecord] = [2]int{
                existing[0] + 1,
                existing[1] + len(finishedGroup),
            }
        } else {
            kmlTallies[cityRecord] = [2]int{
                1,
                len(finishedGroup),
            }
        }
    }

    if len(destPaths) > 0 {
        err := writeCopyPathInfo(groupArguments.CopyPath, destPaths)
        log.PanicIf(err)

        tallies := make(Tallies, len(destPaths))
        i := 0
        for destPath, count := range destPaths {
            destRelPath := destPath[len(groupArguments.CopyPath)+1:]

            tallies[i] = tallyItem{
                name:  destRelPath,
                count: count,
            }

            i++
        }

        // This sorts in reverse.
        sort.Sort(tallies)

        for _, ti := range tallies {
            if ti.count < 50 {
                break
            }

            fmt.Printf("%s: (%d)\n", ti.name, ti.count)
        }

        // TODO(dustin): !! Use an existing tool to generate linked HTML indices for browsing.
    }

    // TODO(dustin): !! Make sure that files that returned nil,nil from the image processor in go-geographic-index is logged as unassigned. OTherwise, we'll have no chance of debugging image issues.

    if groupArguments.JsonFilepath != "" {
        err := writeGroupInfoAsJson(fg, collected, groupArguments.JsonFilepath)
        log.PanicIf(err)
    }

    unassignedRecords := fg.UnassignedRecords()
    if len(unassignedRecords) > 0 && groupArguments.UnassignedFilepath != "" {
        f, err := os.Create(groupArguments.UnassignedFilepath)
        log.PanicIf(err)

        defer f.Close()

        for _, ur := range unassignedRecords {
            fmt.Fprintf(f, "%s\t%s\n", ur.Geographic.Filepath, ur.Reason)
        }
    }

    if groupArguments.KmlFilepath != "" {
        err := writeGroupInfoAsKml(kmlTallies, groupArguments.KmlFilepath)
        log.PanicIf(err)
    }
}

func copyFile(fg *geoautogroup.FindGroups, finishedGroupKey geoautogroup.GroupKey, finishedGroup []geoindex.GeographicRecord, copyRootPath string, imageOutputPathTemplate *template.Template, printDotOutput bool) (destPath string, err error) {
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

    replacements := map[string]interface{}{
        "year":                    timeKey.Year(),
        "month_number":            fmt.Sprintf("%02d", timeKey.Month()),
        "month_name":              fmt.Sprintf("%s", timeKey.Month()),
        "day_number":              fmt.Sprintf("%02d", timeKey.Day()),
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

    destPath = path.Join(copyRootPath, folderName)

    err = os.MkdirAll(destPath, 0755)
    log.PanicIf(err)

    for _, gr := range finishedGroup {
        filename := path.Base(gr.Filepath)
        destFilepath := path.Join(destPath, filename)

        destExt := path.Ext(destFilepath)
        leftSide := destFilepath[:len(destFilepath)-len(destExt)]

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

            // File already exists.

            destFilepath = fmt.Sprintf("%s (%d)%s", leftSide, i+1, destExt)
        }

        if printDotOutput == true {
            fmt.Printf(".")
        }

        fromFile, err := os.Open(gr.Filepath)
        log.PanicIf(err)

        toFile, err := os.Create(destFilepath)
        log.PanicIf(err)

        _, err = io.Copy(toFile, fromFile)
        log.PanicIf(err)

        fromFile.Close()
        toFile.Close()
    }

    if printDotOutput == true {
        fmt.Printf("\n")
    }

    return destPath, nil
}

func writeGroupInfoAsJson(fg *geoautogroup.FindGroups, collected []interface{}, filepath string) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    f, err := os.Create(filepath)
    log.PanicIf(err)

    defer f.Close()

    nearestCityIndex := fg.NearestCityIndex()

    content := map[string]interface{}{
        "groups":     collected,
        "city_index": nearestCityIndex,
    }

    e := json.NewEncoder(f)
    e.SetIndent("", "  ")

    err = e.Encode(content)
    log.PanicIf(err)

    return nil
}

func writeCopyPathInfo(destRootPath string, destPaths map[string]int) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    destPathTallies := make(map[string]int)
    for destPath, _ := range destPaths {
        entries, err := ioutil.ReadDir(destPath)
        log.PanicIf(err)

        destRelPath := destPath[len(destRootPath)+1:]

        destPathTallies[destRelPath] = len(entries)
    }

    timestampPhraseBytes, err := time.Now().MarshalText()
    log.PanicIf(err)

    copyInfoFilename := fmt.Sprintf("%s-%s.json", copyInfoFilenamePrefix, string(timestampPhraseBytes))
    copyInfoFilepath := path.Join(destRootPath, copyInfoFilename)

    f, err := os.Create(copyInfoFilepath)
    log.PanicIf(err)

    defer f.Close()

    e := json.NewEncoder(f)
    e.SetIndent("", "  ")

    err = e.Encode(destPathTallies)
    log.PanicIf(err)

    return nil
}

func writeGroupInfoAsKml(tallies map[geoattractor.CityRecord][2]int, filepath string) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    elements := make([]kml.Element, 0)
    for cr, tallies := range tallies {
        var description string
        if tallies[0] > 1 {
            description = fmt.Sprintf("%d groups<br />%d pictures", tallies[0], tallies[1])
        } else {
            description = fmt.Sprintf("%d pictures", tallies[1])
        }

        coordinate := kml.Coordinate{
            cr.Longitude,
            cr.Latitude,
            0,
        }

        name := cr.CityAndProvinceState()

        groupPoint := kml.Placemark(
            kml.Name(name),
            kml.Description(description),
            // kml.StyleURL("#RedPlaces"),
            kml.Point(
                kml.Coordinates(coordinate),
            ),
        )

        elements = append(elements, groupPoint)
    }

    k := kml.KML(
        kml.Document(
            elements...,
        ),
    )

    // Render the XML.

    f, err := os.Create(filepath)
    log.PanicIf(err)

    defer f.Close()

    e := xml.NewEncoder(f)
    e.Indent("", "  ")

    err = e.Encode(k)
    log.PanicIf(err)

    return nil
}

func main() {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.PrintError(err)
            os.Exit(-1)
        }
    }()

    p := flags.NewParser(rootArguments, flags.Default)

    _, err := p.Parse()
    if err != nil {
        os.Exit(1)
    }

    switch p.Active.Name {
    case "group":
        handleGroup(rootArguments.Group)
    default:
        fmt.Printf("Subcommand not handled: [%s]\n", p.Active.Name)
        os.Exit(2)
    }
}

func init() {
    scp := log.NewStaticConfigurationProvider()
    scp.SetLevelName(log.LevelNameError)

    log.LoadConfiguration(scp)

    cla := log.NewConsoleLogAdapter()
    log.AddAdapter("console", cla)
}
