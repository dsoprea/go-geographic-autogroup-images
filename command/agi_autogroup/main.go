package main

import (
    "fmt"
    "os"
    "path"
    "sort"
    "time"

    "encoding/json"
    "encoding/xml"
    "io/ioutil"
    "text/template"

    "github.com/jessevdk/go-flags"
    "github.com/twpayne/go-kml"

    "github.com/dsoprea/go-geographic-attractor"
    "github.com/dsoprea/go-geographic-attractor/index"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
    "github.com/dsoprea/go-time-parse"

    "github.com/dsoprea/go-geographic-autogroup-images"
)

var (
    mainLogger = log.NewLogger("main")
)

const (
    copyInfoFilenamePrefix     = ".autogroup"
    largestGroupMinimumSize    = 50
    destHtmlCatalogDefaultName = "Grouped Image Catalog"

    catalogImageWidth  = 600
    catalogImageHeight = 0
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
    CountriesFilepath string `long:"countries-filepath" description:"File-path of the GeoNames countries data (usually called 'countryInfo.txt')"`
    CitiesFilepath    string `long:"cities-filepath" description:"File-path of the GeoNames world-cities data (usually called 'allCountries.txt')"`
}

// indexParameters are the parameters common to anything that needs to load a
// `geoindex.GeographicCollector`.
type indexParameters struct {
    DataPaths  []string `long:"data-path" description:"Path to scan for geographic data (GPX files and image files; can be provided more than once)" required:"true"`
    ImagePaths []string `long:"image-path" description:"Path to scan for images to group (can be provided more than once)" required:"true"`
}

type sourceCatalogParameters struct {
    NoEmbedImages bool `long:"no-embed-images" description:"By default thumbnails are embedded directly into the catalog. Use the source image directly, instead."`
}

type groupParameters struct {
    attractorParameters
    indexParameters

    LocationsAreSparse         bool   `long:"sparse-data" description:"Location data is sparse. Sparse datasets will not record points if there has been no movement."`
    KmlFilepath                string `long:"kml-filepath" description:"Write KML to the given file. Enabled by default and named 'groups.kml' in the --copy-into-path argument if provided. Can be disabled using 'none'."`
    KmlMinimumGroupImageCount  int    `long:"kml-minimum" description:"Exclude groups with less than N images from the KML" default:"20"`
    JsonFilepath               string `long:"json-filepath" description:"Write JSON to the given file. Enabled by default and named 'groups.json' in the --copy-into-path argument if provided. Can be disabled using 'none'."`
    UnassignedFilepath         string `long:"unassigned-filepath" description:"File to write unassigned files to. Enabled by default and named 'unassigned.txt' in --copy-into-path argument if provided."`
    PrintStats                 bool   `long:"stats" description:"Print statistics"`
    CopyPath                   string `long:"copy-into-path" description:"Copy grouped images into this path"`
    ImageOutputPathTemplate    string `long:"output-template" description:"Group output path name template within the output path. Can use Go template tokens." default:"{{.year}}-{{.month_number}}-{{.day_number}} {{.location}}{{.path_sep}}{{.camera_model}}/{{.hour}}.{{.minute}}"`
    NoPrintProgressOutput      bool   `long:"no-dots" description:"Don't print dot progress output if copying"`
    NoHashChecksOnExisting     bool   `long:"no-hash-checks" description:"If the file already exists in copy-path skip without calculating hash"`
    ImageTimestampSkewRaw      string `long:"image-timestamp-skew" description:"A duration to add to the timestamps of the images to compensate for their timezones. By default, all images are interpreted as UTC (a requirement of EXIF). Example: 5h"`
    ImageTimestampSkewPolarity bool   `long:"image-timestamp-skew-polarity" description:"If skew is being used, true if it should be negative and false if positive"`

    sourceCatalogParameters
}

type subcommands struct {
    Group groupParameters `command:"group" description:"Grouping operations"`
}

var (
    rootArguments = new(subcommands)
)

func getFindGroups(groupArguments groupParameters) (fg *geoautogroup.FindGroups, ci *geoattractorindex.CityIndex) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    ci, err := geoautogroup.GetCityIndex(groupArguments.attractorParameters.CountriesFilepath, groupArguments.attractorParameters.CitiesFilepath)
    log.PanicIf(err)

    locationIndex, err := geoautogroup.GetTimeIndex(groupArguments.indexParameters.DataPaths, 0)
    log.PanicIf(err)

    locationTs := locationIndex.Series()

    if groupArguments.PrintStats == true {
        fmt.Printf("(%d) records loaded in location index.\n", len(locationTs))
    }

    var imageTimestampSkew time.Duration
    if groupArguments.ImageTimestampSkewRaw != "" {
        var err error
        imageTimestampSkew, _, err = timeparse.ParseDuration(groupArguments.ImageTimestampSkewRaw)
        log.PanicIf(err)

        if groupArguments.ImageTimestampSkewPolarity == true {
            imageTimestampSkew *= -1
        }
    }

    imageIndex, err := geoautogroup.GetTimeIndex(groupArguments.indexParameters.ImagePaths, imageTimestampSkew)
    log.PanicIf(err)

    imageTs := imageIndex.Series()

    if groupArguments.PrintStats == true {
        fmt.Printf("(%d) records loaded in image index.\n", len(imageTs))
    }

    fg = geoautogroup.NewFindGroups(locationTs, imageTs, ci)

    if groupArguments.LocationsAreSparse == true {
        fg.SetLocationMatchStrategy(geoautogroup.LocationMatchStrategySparseData)
    }

    return fg, ci
}

type imageFileMapping struct {
    OutputFilepath              string
    RelativeFilepathFromCatalog string
}

func handleGroup(groupArguments groupParameters) {
    defer func() {
        if state := recover(); state != nil {
            err := log.Wrap(state.(error))
            log.Panic(err)
        }
    }()

    sessionTimestampPhrase := geoautogroup.GetCondensedDatetime(time.Now())

    fg, ci := getFindGroups(groupArguments)

    // Run the grouping operation.

    gr := geoautogroup.NewGroupsReducer(fg)

    // Merge smaller cities with smaller datasets into the groups for larger
    // cities.

    collectedGroups, merged := gr.Reduce()

    if merged > 0 {
        keptCount := 0
        for _, groups := range collectedGroups {
            keptCount += len(groups)
        }

        if groupArguments.PrintStats == true {
            fmt.Printf("Coalesced (%d) trivial groups. There are (%d) final groups.\n", merged, keptCount)
            fmt.Printf("\n")
        }
    }

    // Copy images.

    if groupArguments.CopyPath != "" {
        fmt.Printf("Copying images:\n")
        fmt.Printf("\n")
    }

    kmlTallies := make(map[geoattractor.CityRecord][2]int)
    collected := make([]map[string]interface{}, 0)

    imageOutputPathTemplate := template.Must(template.New("group path template").Parse(groupArguments.ImageOutputPathTemplate))

    printProgressOutput := (groupArguments.NoPrintProgressOutput == false)

    // Depending on the folder-template, the groups may, and probably will, be
    // merged-together to some degree on disk (our groups might differ in
    // minutes but may be stored on disk by date). So, keep track of them based
    // on the on-disk folder rather than by their in-memory names (found via
    // `String()`).
    binnedImages := make(map[string][]*geoindex.GeographicRecord)

    fileMappings := make(map[string]imageFileMapping)
    i := 0
    for _, groups := range collectedGroups {
        for _, cg := range groups {
            finishedGroupKey := cg.GroupKey
            finishedGroup := cg.Records

            if groupArguments.CopyPath != "" {
                err := copyFiles(groupArguments, fg, finishedGroupKey, finishedGroup, groupArguments.CopyPath, imageOutputPathTemplate, printProgressOutput, binnedImages, fileMappings)
                log.PanicIf(err)
            }

            if collected != nil {
                item := map[string]interface{}{
                    "group_key": finishedGroupKey,
                    "records":   finishedGroup,
                }

                collected = append(collected, item)
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

            i++
        }
    }

    if groupArguments.PrintStats == true {
        fmt.Printf("\n")
        fmt.Printf("Attractor index stats: %s\n", ci.Stats())
    }

    urbanCenters := fg.UrbanCentersEncountered()
    if len(urbanCenters) > 0 {
        fmt.Printf("\n")
        fmt.Printf("Urban Areas Visited\n")
        fmt.Printf("===================\n")

        ids := make(sort.StringSlice, len(urbanCenters))
        i := 0
        for id, _ := range urbanCenters {
            ids[i] = id
            i++
        }

        ids.Sort()

        for _, id := range ids {
            cr := urbanCenters[id]
            fmt.Printf("%8s  %s  (%.6f,%.6f)\n", cr.Id, cr.CityAndProvinceState(), cr.Latitude, cr.Longitude)
        }
    }

    if collected != nil {
        // Automatically write a destination catalog if we're doing a copy.
        if groupArguments.CopyPath != "" {
            destCatalogPath := path.Join(groupArguments.CopyPath, "catalog", sessionTimestampPhrase)

            fmt.Printf("\n")
            fmt.Printf("Writing catalog to: %s\n", destCatalogPath)

            err := writeDestHtmlCatalog(groupArguments, fg, collected, destCatalogPath, groupArguments.NoEmbedImages, fileMappings)
            log.PanicIf(err)
        }
    }

    if len(binnedImages) > 0 {
        err := writeCopyPathInfo(groupArguments, sessionTimestampPhrase, groupArguments.CopyPath, binnedImages)
        log.PanicIf(err)

        tallies := make(Tallies, 0)
        for folderName, entries := range binnedImages {
            count := len(entries)
            if count < largestGroupMinimumSize {
                continue
            }

            ti := tallyItem{
                name:  folderName,
                count: count,
            }

            tallies = append(tallies, ti)
        }

        if len(tallies) > 0 {
            // This sorts in reverse.
            sort.Sort(tallies)

            fmt.Printf("\n")
            fmt.Printf("Largest Groups\n")
            fmt.Printf("==============\n")

            for _, ti := range tallies {
                fmt.Printf("%s: (%d)\n", ti.name, ti.count)
            }
        }
    }

    // TODO(dustin): !! Make sure that files that returned nil,nil from the image processor in go-geographic-index is logged as unassigned. Otherwise, we'll have no chance of debugging image issues.

    jsonFilepath := groupArguments.JsonFilepath
    if jsonFilepath == "" {
        if groupArguments.CopyPath != "" {
            jsonFilepath = path.Join(groupArguments.CopyPath, "groups.json")
        } else {
            jsonFilepath = "none"
        }
    }

    if jsonFilepath != "none" {
        // Write all of the final data as a JSON structure.

        encodedGroups := make([]map[string]interface{}, len(collected))
        for i, groupInfo := range collected {
            groupKey := groupInfo["group_key"].(geoautogroup.GroupKey)
            originalRecords := groupInfo["records"].([]*geoindex.GeographicRecord)

            updatedRecords := make([]map[string]interface{}, len(originalRecords))
            locationSourceRecords := make(map[string]map[string]interface{})
            for i, gr := range originalRecords {
                encoded := gr.Encode()

                // Relocate relationships to reduce duplication and clutter.

                encodedRelationships := encoded["relationships"].(map[string][]map[string]interface{})

                if len(encodedRelationships) == 0 {
                    delete(encodedRelationships, "relationships")
                } else {
                    updatedRelationships := make(map[string][]string)
                    for type_, encodedGrList := range encodedRelationships {
                        filepaths := make([]string, len(encodedGrList))
                        for i, encodedGr := range encodedGrList {
                            filepath := encodedGr["filepath"].(string)
                            filepaths[i] = filepath
                            locationSourceRecords[filepath] = encodedGr
                        }

                        updatedRelationships[type_] = filepaths
                    }

                    encoded["relationships"] = updatedRelationships
                }

                updatedRecords[i] = encoded
            }

            item := map[string]interface{}{
                "group_key":        groupKey,
                "records":          updatedRecords,
                "location_sources": locationSourceRecords,
            }

            encodedGroups[i] = item
        }

        err := writeGroupInfoAsJson(fg, encodedGroups, jsonFilepath)
        log.PanicIf(err)
    }

    unassignedRecords := fg.UnassignedRecords()

    len_ := len(unassignedRecords)
    if len_ > 0 {
        fmt.Printf("(%d) records could not be matched with locations.\n", len_)
        fmt.Printf("\n")

        unassignedFilepath := groupArguments.UnassignedFilepath
        if unassignedFilepath == "" {
            if groupArguments.CopyPath != "" {
                unassignedFilepath = path.Join(groupArguments.CopyPath, "unassigned.txt")
            } else {
                unassignedFilepath = "none"
            }
        }

        if unassignedFilepath != "none" {
            f, err := os.Create(unassignedFilepath)
            log.PanicIf(err)

            defer f.Close()

            for _, ur := range unassignedRecords {
                fmt.Fprintf(f, "%s\t%s\n", ur.Geographic.Filepath, ur.Reason)
            }
        }
    }

    kmlFilepath := groupArguments.KmlFilepath
    if kmlFilepath == "" {
        if groupArguments.CopyPath != "" {
            kmlFilepath = path.Join(groupArguments.CopyPath, "groups.kml")
        } else {
            kmlFilepath = "none"
        }
    }

    if kmlFilepath != "none" {
        err := writeGroupInfoAsKml(kmlTallies, kmlFilepath)
        log.PanicIf(err)
    }
}

func writeGroupInfoAsJson(fg *geoautogroup.FindGroups, collected []map[string]interface{}, filepath string) (err error) {
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

func writeCopyPathInfo(groupArguments groupParameters, sessionTimestampPhrase, destRootPath string, binnedImages map[string][]*geoindex.GeographicRecord) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    destPathTallies := make(map[string]int)
    for folderName, _ := range binnedImages {
        destPath := path.Join(groupArguments.CopyPath, folderName)

        entries, err := ioutil.ReadDir(destPath)
        log.PanicIf(err)

        destRelPath := destPath[len(destRootPath)+1:]

        destPathTallies[destRelPath] = len(entries)
    }

    copyInfoFilename := fmt.Sprintf("%s-%s.json", copyInfoFilenamePrefix, sessionTimestampPhrase)
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
        fmt.Printf("%s\n", err)
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
