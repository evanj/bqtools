package bqscrape

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"google.golang.org/api/gensupport"

	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

// BigQuery permits 100 requests per second per user: use half at max
// https://cloud.google.com/bigquery/quota-policy#apirequests
const requestPerSecondLimit = rate.Limit(50)
const maxConcurrentAPIRequests = 10
const maxDatasets = 500

// In 1 hour of trying to read a project with 319598 tables: couldn't get past 40k
const maxTables = 20000

// https://cloud.google.com/bigquery/docs/data#paging-through-list-results
const collectionMaxResults = 1000

// https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
const TypeTable = "TABLE"

// Makes it easier to test this code
type api interface {
	listDatasets(projectId string, pageToken string) (*bigquery.DatasetList, error)
	listTables(projectId string, datasetId string, pageToken string) (*bigquery.TableList, error)
	getTable(projectId string, datasetId string, tableId string) (*bigquery.Table, error)
}

type bigQueryAPI struct {
	bq *bigquery.Service
}

func (a *bigQueryAPI) listDatasets(projectId string, pageToken string) (
	*bigquery.DatasetList, error) {
	// TODO: filter attributes?
	request := a.bq.Datasets.List(projectId).
		PageToken(pageToken).
		MaxResults(collectionMaxResults)

	var result *bigquery.DatasetList
	makeRequest := func() error {
		var err error
		result, err = request.Do()
		return err
	}
	err := retry(context.TODO(), makeRequest)
	return result, err
}

func (a *bigQueryAPI) listTables(projectId string, datasetId string, pageToken string) (
	*bigquery.TableList, error) {
	// TODO: filter attributes?
	request := a.bq.Tables.List(projectId, datasetId).
		PageToken(pageToken).
		MaxResults(collectionMaxResults)

	var result *bigquery.TableList
	makeRequest := func() error {
		var err error
		result, err = request.Do()
		return err
	}
	err := retry(context.TODO(), makeRequest)
	return result, err
}

func (a *bigQueryAPI) getTable(projectId string, datasetId string, tableId string) (
	*bigquery.Table, error) {
	request := a.bq.Tables.Get(projectId, datasetId, tableId).
		// created with the API fields editor
		Fields("creationTime,description,expirationTime,friendlyName,id,kind,lastModifiedTime,numBytes,numLongTermBytes,numRows,streamingBuffer,tableReference,type")

	var result *bigquery.Table
	makeRequest := func() error {
		var err error
		result, err = request.Do()
		return err
	}
	err := retry(context.TODO(), makeRequest)
	return result, err
}

var knownPermanent = map[string]int{
	"accessDenied": http.StatusForbidden,
}

// Retries f until it hits a permanent error or times out. Based on gensupport.Retry
func retry(ctx context.Context, f func() error) error {
	// around 4 retries: 100, 200, 400, 800 = 1500 ms; expected value of each wait is half
	// TODO: gensupport.ExponentialBackoff will wait one pause longer than the max time; do we care?
	backoff := gensupport.ExponentialBackoff{
		Base: 100 * time.Millisecond,
		Max:  1 * time.Second,
	}

	for {
		err := f()
		if err == nil {
			return nil
		}

		// Return the error if we shouldn't retry
		pause, retry := backoff.Pause()
		if !retry || isPermanentErr(err) {
			return err
		}

		// Pause, but still listen to ctx.Done
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
	}
}

// Returns true if err is a permanent bigquery error. If it returns false, it means we aren't
// sure, so it should be okay to retry. see the unit test for example errors we have hit.
func isPermanentErr(err error) bool {
	if apiErr, ok := err.(*googleapi.Error); ok {
		firstReason := ""
		if len(apiErr.Errors) > 0 {
			firstReason = apiErr.Errors[0].Reason
			if len(apiErr.Errors) > 1 {
				reasons := []string{}
				for _, detailedError := range apiErr.Errors {
					reasons = append(reasons, detailedError.Reason)
				}
				log.Printf("bqscrape warning: found multiple reasons: %v error: %s", reasons, apiErr.Error())
			}
		} else {
			log.Printf("bqscrape warning: found error without reasons: %s", apiErr.Error())
		}
		_, isPermanent := knownPermanent[firstReason]

		log.Printf("bqscape: warning googleapi.Error: isPermanent: %v; reason: %s; %s",
			isPermanent, firstReason, apiErr.Error())
		return isPermanent
	} else if urlErr, ok := err.(*url.Error); ok {
		isPermanent := !(urlErr.Temporary() || urlErr.Err == io.ErrUnexpectedEOF)
		log.Printf("bqscape: warning url.Error: isPermanent: %v; temporary: %v timeout: %v; %s",
			isPermanent, urlErr.Temporary(), urlErr.Timeout(), urlErr.Error())
		return isPermanent
	}

	// default to not permanent: it is safe but wasteful to "incorrectly" retry permanent errors;
	// it is worse to incorrectly not retry a temporary error
	log.Printf("bqscrape: warning isPermanentErr(type %s): unhandled: %s",
		reflect.TypeOf(err).String(), err.Error())
	return false
}

func listAllDatasets(bqAPI api, projectId string, limiter *rate.Limiter) (
	[]*bigquery.DatasetListDatasets, error) {

	var datasets []*bigquery.DatasetListDatasets
	nextPageToken := ""
	for {
		err := limiter.Wait(context.TODO())
		resp, err := bqAPI.listDatasets(projectId, nextPageToken)
		if err != nil {
			return nil, err
		}

		log.Printf("bqscrape: project %s: %d datasets in page", projectId, len(resp.Datasets))
		datasets = append(datasets, resp.Datasets...)
		if len(datasets) > maxDatasets {
			return nil, fmt.Errorf("bqscrape: projectId:%s exceeded max datasets:%d",
				projectId, maxDatasets)
		}
		nextPageToken = resp.NextPageToken
		if nextPageToken == "" {
			break
		}
	}
	return datasets, nil
}

// Appends all tables in the dataset to tables.
func appendTablesInDataset(tables []*bigquery.TableListTables, bqAPI api, projectId string,
	datasetId string, limiter *rate.Limiter) ([]*bigquery.TableListTables, error) {

	nextPageToken := ""
	for {
		err := limiter.Wait(context.TODO())
		resp, err := bqAPI.listTables(projectId, datasetId, nextPageToken)
		if err != nil {
			return nil, err
		}

		log.Printf("bqscrape: project %s dataset %s: %d tables in page",
			projectId, datasetId, len(resp.Tables))
		tables = append(tables, resp.Tables...)
		if len(tables) > maxTables {
			return nil, fmt.Errorf("bqscrape: projectId:%s exceeded max tables:%d", projectId, maxTables)
		}
		nextPageToken = resp.NextPageToken
		if nextPageToken == "" {
			break
		}
	}
	return tables, nil
}

func listAllTables(bqAPI api, projectId string, limiter *rate.Limiter) (
	[]*bigquery.TableListTables, error) {

	datasets, err := listAllDatasets(bqAPI, projectId, limiter)
	if err != nil {
		return nil, err
	}

	tables := []*bigquery.TableListTables{}
	for _, dataset := range datasets {
		datasetID := dataset.DatasetReference.DatasetId
		tables, err = appendTablesInDataset(tables, bqAPI, projectId, datasetID, limiter)
		if err != nil {
			return nil, err
		}
	}
	return tables, nil
}

// assume listing datasets and tables is 10%; saving is 1% (definitely wrong)
const listTablesPercent = 10
const savingPercent = 1
const progressTableCount = 100

func estimateListTablesProgress(tablesListed int, totalTables int) (int, string) {
	fraction := float64(tablesListed) / float64(totalTables)
	percent := listTablesPercent + int((100-listTablesPercent-savingPercent)*fraction)
	message := fmt.Sprintf("Reading table metadata: %d/%d tables",
		tablesListed, totalTables)
	return percent, message
}

// Fetches all metadata from all bigquery tables from projectId. TODO: Parallelize
func getAllTables(bqAPI api, projectId string, limiter *rate.Limiter, progress ProgressReporter) (
	[]*bigquery.Table, error) {

	const listTablesPercent = 10

	progress.Progress(0, "Listing tables...")
	tables, err := listAllTables(bqAPI, projectId, limiter)
	if err != nil {
		return nil, err
	}

	tableData := make([]*bigquery.Table, len(tables))
	for i, table := range tables {
		// report progress every 100 tables
		if i%progressTableCount == 0 {
			percent, message := estimateListTablesProgress(i, len(tables))
			progress.Progress(percent, message)
		}

		tableData[i], err = bqAPI.getTable(table.TableReference.ProjectId, table.TableReference.DatasetId,
			table.TableReference.TableId)
		if err != nil {
			return nil, err
		}
	}
	// TODO: factor this into the progress indicator better
	progress.Progress(99, "Saving results...")
	return tableData, nil
}

func productionConfig(bq *bigquery.Service) (api, *rate.Limiter) {
	bqAPI := &bigQueryAPI{bq}
	// burst = per second rate means worst case we send 2X requests in the first second
	limiter := rate.NewLimiter(requestPerSecondLimit, int(requestPerSecondLimit))
	return bqAPI, limiter
}

// Fetches all bigquery tables from projectId.
func ListAllTables(bq *bigquery.Service, projectId string) ([]*bigquery.TableListTables, error) {
	bqAPI, limiter := productionConfig(bq)
	return listAllTables(bqAPI, projectId, limiter)
}

type ProgressReporter interface {
	Progress(percent int, message string)
}

type NilProgressReporter struct{}

func (n *NilProgressReporter) Progress(percent int, message string) {}

// Fetches all metadata from all bigquery tables from projectId. TODO: Parallelize
func GetAllTables(bq *bigquery.Service, projectId string, progress ProgressReporter) (
	[]*bigquery.Table, error) {

	bqAPI, limiter := productionConfig(bq)
	if progress == nil {
		progress = &NilProgressReporter{}
	}
	return getAllTables(bqAPI, projectId, limiter, progress)
}
