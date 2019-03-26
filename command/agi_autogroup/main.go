package main

import (
    "bytes"
    "fmt"
    "io"
    "os"
    "path"
    "sort"
    "time"

    "crypto/sha1"
    "encoding/json"
    "encoding/xml"
    "io/ioutil"
    "text/template"

    "github.com/dsoprea/go-logging"
    "github.com/dsoprea/go-static-site-builder"
    "github.com/dsoprea/go-static-site-builder/markdown"
    "github.com/jessevdk/go-flags"
    "github.com/sbwhitecap/tqdm"
    "github.com/sbwhitecap/tqdm/iterators"
    "github.com/twpayne/go-kml"

    "github.com/dsoprea/go-geographic-attractor"
    "github.com/dsoprea/go-geographic-autogroup-images"
    "github.com/dsoprea/go-geographic-index"
)

var (
    mainLogger = log.NewLogger("main")
)

const (
    copyInfoFilenamePrefix     = ".autogroup"
    largestGroupSizeMinimum    = 50
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

    LocationsAreSparse        bool   `long:"sparse-data" description:"Location data is sparse. Sparse datasets will not record points if there has been no movement."`
    KmlFilepath               string `long:"kml-filepath" description:"Write KML to the given file"`
    KmlMinimumGroupImageCount int    `long:"kml-minimum" description:"Exclude groups with less than N images from the KML" default:"20"`
    JsonFilepath              string `long:"json-filepath" description:"Write JSON to the given file"`
    UnassignedFilepath        string `long:"unassigned-filepath" description:"File to write unassigned files to"`
    PrintStats                bool   `long:"stats" description:"Print statistics"`
    CopyPath                  string `long:"copy-into-path" description:"Copy grouped images into this path."`
    ImageOutputPathTemplate   string `long:"output-template" description:"Group output path name template within the output path. Can use Go template tokens." default:"{{.year}}-{{.month_number}}-{{.day_number}} {{.location}}{{.path_sep}}{{.camera_model}}/{{.hour}}.{{.minute}}"`
    NoPrintProgressOutput     bool   `long:"no-dots" description:"Don't print dot progress output if copying"`

    sourceCatalogParameters
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

    locationIndex, err := geoautogroup.GetTimeIndex(groupArguments.indexParameters.DataPaths)
    log.PanicIf(err)

    locationTs := locationIndex.Series()

    if groupArguments.PrintStats == true {
        fmt.Printf("(%d) records loaded in location index.\n", len(locationTs))
    }

    imageIndex, err := geoautogroup.GetTimeIndex(groupArguments.indexParameters.ImagePaths)
    log.PanicIf(err)

    imageTs := imageIndex.Series()

    if groupArguments.PrintStats == true {
        fmt.Printf("(%d) records loaded in image index.\n", len(imageTs))
    }

    fg = geoautogroup.NewFindGroups(locationTs, imageTs, ci)

    if groupArguments.LocationsAreSparse == true {
        fg.SetLocationMatchStrategy(geoautogroup.LocationMatchStrategySparseData)
    }

    return fg
}

type catalogItem struct {
    groupKey   geoautogroup.GroupKey
    linkWidget sitebuilder.LinkWidget
}

// sortableLinks is a sortable slice of catalog items. We use it to sort the
// catalog in a sensible order.
type sortableLinks []catalogItem

func (sl sortableLinks) Len() int {
    return len(sl)
}

func (sl sortableLinks) Swap(i, j int) {
    sl[i], sl[j] = sl[j], sl[i]
}

func (sl sortableLinks) Less(i, j int) bool {
    first, second := sl[i], sl[j]

    if first.groupKey.TimeKey != second.groupKey.TimeKey {
        return first.groupKey.TimeKey.Before(second.groupKey.TimeKey)
    }

    // The time matches. Compare the city.

    if first.groupKey.NearestCityKey != second.groupKey.NearestCityKey {
        return first.groupKey.NearestCityKey < second.groupKey.NearestCityKey
    }

    return first.groupKey.CameraModel < second.groupKey.CameraModel
}

// writeDestHtmlCatalog will write an HTML catalog to the disk. Note that the
// catalog is organized by original groups whereas the the physical folders on
// the disk may or may not be combined based on the folder-name template.
func writeDestHtmlCatalog(groupArguments groupParameters, fg *geoautogroup.FindGroups, collected []map[string]interface{}, copyPath string, noEmbedImages bool, fileMappings map[string]imageFileMapping) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // TODO(dustin): !! Finish implementing `noEmbedImages`.

    sc := sitebuilder.NewSiteContext(copyPath)
    md := markdowndialect.NewMarkdownDialect()

    sb := sitebuilder.NewSiteBuilder(destHtmlCatalogDefaultName, md, sc)

    // Create content on root page.

    rootNode := sb.Root()

    nearestCityIndex := fg.NearestCityIndex()

    catalogItems := make([]catalogItem, 0)
    for _, item := range collected {
        groupKey := item["group_key"].(geoautogroup.GroupKey)
        groupedItems := item["records"].([]*geoindex.GeographicRecord)

        cityRecord := nearestCityIndex[groupKey.NearestCityKey]

        timePhrase := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", groupKey.TimeKey.Year(), groupKey.TimeKey.Month(), groupKey.TimeKey.Day(), groupKey.TimeKey.Hour(), groupKey.TimeKey.Minute(), groupKey.TimeKey.Second())
        childPageTitle := fmt.Sprintf("%s UTC (%s) %s", timePhrase, cityRecord.CityAndProvinceState(), groupKey.CameraModel)

        navbarTitle := fmt.Sprintf("%s (%d)", childPageTitle, len(groupedItems))

        childPageId, err := writeDestHtmlCatalogGroup(rootNode, groupKey, cityRecord, childPageTitle, groupedItems, fileMappings)
        log.PanicIf(err)

        catalogLw := sitebuilder.NewLinkWidget(navbarTitle, sitebuilder.NewSitePageLocalResourceLocator(sb, childPageId))

        ci := catalogItem{
            groupKey:   groupKey,
            linkWidget: catalogLw,
        }

        catalogItems = append(catalogItems, ci)
    }

    stl := sortableLinks(catalogItems)
    sort.Sort(stl)

    catalogLinks := make([]sitebuilder.LinkWidget, len(stl))
    for i, ci := range stl {
        catalogLinks[i] = ci.linkWidget
    }

    rootPb := rootNode.Builder()

    // Add navbar with page links.

    nw := sitebuilder.NewNavbarWidget(catalogLinks)

    // TODO(dustin): !! Replace this with a list of descriptions and the first image.
    err = rootPb.AddVerticalNavbar(nw, "Groups")
    log.PanicIf(err)

    // Render and write.

    err = sb.WriteToPath()
    log.PanicIf(err)

    return nil
}

func writeDestHtmlCatalogGroup(rootNode *sitebuilder.SiteNode, groupKey geoautogroup.GroupKey, cr geoattractor.CityRecord, pageTitle string, groupedItems []*geoindex.GeographicRecord, fileMappings map[string]imageFileMapping) (childPageId string, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // Add a new page.

    childPageId = groupKey.KeyPhrase()
    childNode, err := rootNode.AddChildNode(childPageId, pageTitle)
    log.PanicIf(err)

    childPb := childNode.Builder()

    // Add images.

    for _, gr := range groupedItems {
        imageLocations, found := fileMappings[gr.Filepath]
        if found == false {
            log.Panicf("Could not find copied file-path for [%s] out of (%d) mappings.", gr.Filepath, len(fileMappings))
        }

        lrl := sitebuilder.NewLocalResourceLocator(imageLocations.RelativeFilepathFromCatalog)

        filename := path.Base(imageLocations.RelativeFilepathFromCatalog)

        // TODO(dustin): !! Insert descriptions for each image.
        // TODO(dustin): !! We should also take a nil-able link that we will link the image against if present.
        iw := sitebuilder.NewImageWidget(filename, lrl, catalogImageWidth, catalogImageHeight)

        err = childPb.AddContentImage(iw)
        log.PanicIf(err)
    }

    return childPageId, nil
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

    fg := getFindGroups(groupArguments)

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
    for i := 0; ; i++ {
        finishedGroupKey, finishedGroup, err := fg.FindNext()
        if err != nil {
            if err == geoautogroup.ErrNoMoreGroups {
                break
            }

            log.Panic(err)
        }

        if groupArguments.CopyPath != "" {
            err := copyFiles(fg, finishedGroupKey, finishedGroup, groupArguments.CopyPath, imageOutputPathTemplate, printProgressOutput, binnedImages, fileMappings)
            log.PanicIf(err)
        }

        if collected != nil {
            item := map[string]interface{}{
                "group_key": finishedGroupKey,
                "records":   finishedGroup,
            }

            collected = append(collected, item)
        }

        // TODO(dustin): Just to get rid of incidental pictures from the journey.
        if len(finishedGroup) < groupArguments.KmlMinimumGroupImageCount {
            continue
        }

        // TODO(dustin): !! Why are we not seeing tallies?

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
            if count < largestGroupSizeMinimum {
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

    // TODO(dustin): !! Make sure that files that returned nil,nil from the image processor in go-geographic-index is logged as unassigned. OTherwise, we'll have no chance of debugging image issues.

    if groupArguments.JsonFilepath != "" {
        encodedGroups := make([]map[string]interface{}, len(collected))
        for i, groupInfo := range collected {
            groupKey := groupInfo["group_key"].(geoautogroup.GroupKey)
            originalRecords := groupInfo["records"].([]*geoindex.GeographicRecord)

            updatedRecords := make([]map[string]interface{}, len(originalRecords))
            locationSourceRecords := make(map[string]map[string]interface{})
            for i, gr := range originalRecords {
                encoded := gr.Encode()

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
                "group_key": groupKey,
                "records":   updatedRecords,
                "locations": locationSourceRecords,
            }

            encodedGroups[i] = item
        }

        err := writeGroupInfoAsJson(fg, encodedGroups, groupArguments.JsonFilepath)
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

func copyFiles(fg *geoautogroup.FindGroups, finishedGroupKey geoautogroup.GroupKey, finishedGroup []*geoindex.GeographicRecord, copyRootPath string, imageOutputPathTemplate *template.Template, printProgressOutput bool, binnedImages map[string][]*geoindex.GeographicRecord, fileMappings map[string]imageFileMapping) (err error) {
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
            list = append(list, gr)
        } else {
            binnedImages[folderName] = []*geoindex.GeographicRecord{
                gr,
            }
        }

        filename := path.Base(gr.Filepath)

        finalFilename, err := copyFile(destPath, filename, gr, fileMappings)
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

func copyFile(destPath, filename string, gr *geoindex.GeographicRecord, fileMappings map[string]imageFileMapping) (finalFilename string, err error) {
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
