package bqscrape

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"golang.org/x/time/rate"

	bigquery "google.golang.org/api/bigquery/v2"
)

type fakeBigQueryAPI struct {
	err        error
	pages      []*bigquery.DatasetList
	tablePages map[string]*bigquery.TableList
}

func (a *fakeBigQueryAPI) listDatasets(projectId string, pageToken string) (
	*bigquery.DatasetList, error) {
	if a.err != nil {
		return nil, a.err
	}

	// check that the caller sets the correct NextPageToken
	nextPageToken := ""
	for _, page := range a.pages {
		if nextPageToken == pageToken {
			return page, nil
		}
		nextPageToken = page.NextPageToken
	}
	return nil, fmt.Errorf("could not find page %s", pageToken)
}

func (a *fakeBigQueryAPI) listTables(projectId string, datasetId, pageToken string) (
	*bigquery.TableList, error) {
	panic("TODO: implement")
}

func makePages(projectId string, numPages int, datasetsPerPage int) []*bigquery.DatasetList {
	pages := []*bigquery.DatasetList{}
	for i := 0; i < numPages; i++ {
		page := &bigquery.DatasetList{}
		if i > 0 {
			pages[i-1].NextPageToken = strconv.Itoa(i)
		}

		for j := 0; j < datasetsPerPage; j++ {
			dsId := "ds" + strconv.Itoa(i*datasetsPerPage+j)
			ds := &bigquery.DatasetListDatasets{
				DatasetReference: &bigquery.DatasetReference{ProjectId: "p", DatasetId: dsId},
			}
			page.Datasets = append(page.Datasets, ds)
		}
		pages = append(pages, page)
	}
	return pages
}

func TestListAllDatasets(t *testing.T) {
	fakeBQ := &fakeBigQueryAPI{}
	limiter := rate.NewLimiter(rate.Inf, 0)

	// check that listing a few pages works
	fakeBQ.pages = makePages("project", 3, 2)
	datasets, err := listAllDatasets(fakeBQ, "project", limiter)
	if len(datasets) != 6 || err != nil {
		t.Fatal(datasets, err)
	}
	if datasets[5].DatasetReference.DatasetId != "ds5" {
		t.Error(datasets[5].DatasetReference)
	}

	// check that an error returns the error
	fakeBQ.err = errors.New("foo")
	datasets, err = listAllDatasets(fakeBQ, "project", limiter)
	if datasets != nil || err != fakeBQ.err {
		t.Error(datasets, err)
	}

	// check that we are actually rate limited TODO: fake time?
	msLimiter := rate.NewLimiter(rate.Limit(1000), 1)
	// 3 pages: must take at least 2 ms (0 wait for first, 1 ms, 1 ms)
	fakeBQ.err = nil
	start := time.Now()
	datasets, err = listAllDatasets(fakeBQ, "project", msLimiter)
	end := time.Now()
	if len(datasets) != 6 || err != nil {
		t.Error(datasets, err)
	}
	if end.Sub(start) < 2*time.Millisecond {
		t.Error("not rate limited:", end.Sub(start))
	}

	// check that we fail if we return too many datasets
	fakeBQ.pages = makePages("project", 2, maxDatasets)
	datasets, err = listAllDatasets(fakeBQ, "project", limiter)
	if datasets != nil || err == nil {
		t.Error(datasets, err)
	}
}
