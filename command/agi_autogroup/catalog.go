package main

import (
    "fmt"
    "path"
    "sort"

    "github.com/dsoprea/go-geographic-attractor"
    "github.com/dsoprea/go-geographic-index"
    "github.com/dsoprea/go-logging"
    "github.com/dsoprea/go-static-site-builder"
    "github.com/dsoprea/go-static-site-builder/markdown"

    "github.com/dsoprea/go-geographic-autogroup-images"
)

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

        localTimeKey := groupKey.TimeKey.Local()

        tzName, _ := localTimeKey.Zone()
        timePhrase := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d %s", localTimeKey.Year(), localTimeKey.Month(), localTimeKey.Day(), localTimeKey.Hour(), localTimeKey.Minute(), localTimeKey.Second(), tzName)
        childPageTitle := fmt.Sprintf("%s (%s) %s", timePhrase, cityRecord.CityAndProvinceState(), groupKey.CameraModel)

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
