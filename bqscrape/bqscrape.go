package bqscrape

import (
	"context"
	"fmt"
	"log"

	"golang.org/x/time/rate"
	"google.golang.org/api/bigquery/v2"
)

// BigQuery permits 100 requests per second per user: use half at max
// https://cloud.google.com/bigquery/quota-policy#apirequests
const requestPerSecondLimit = rate.Limit(50)
const maxConcurrentAPIRequests = 10
const maxDatasets = 100
const maxTables = 1000

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
	// TODO: filter attributes? Set max results
	return a.bq.Datasets.List(projectId).PageToken(pageToken).Do()
}

func (a *bigQueryAPI) listTables(projectId string, datasetId string, pageToken string) (
	*bigquery.TableList, error) {
	// TODO: filter attributes? Set max results
	return a.bq.Tables.List(projectId, datasetId).PageToken(pageToken).Do()
}

func (a *bigQueryAPI) getTable(projectId string, datasetId string, tableId string) (
	*bigquery.Table, error) {
	// TODO: filter attributes?
	return a.bq.Tables.Get(projectId, datasetId, tableId).Do()
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

		log.Printf("bqscrape: %d datasets in page", len(resp.Datasets))
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

		log.Printf("bqscrape: %d tables in page", len(resp.Tables))
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

// Fetches all metadata from all bigquery tables from projectId. TODO: Parallelize
func getAllTables(bqAPI api, projectId string, limiter *rate.Limiter) ([]*bigquery.Table, error) {
	tables, err := listAllTables(bqAPI, projectId, limiter)
	if err != nil {
		return nil, err
	}

	tableData := make([]*bigquery.Table, len(tables))
	for i, table := range tables {
		tableData[i], err = bqAPI.getTable(table.TableReference.ProjectId, table.TableReference.DatasetId,
			table.TableReference.TableId)
		if err != nil {
			return nil, err
		}
	}
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

// Fetches all metadata from all bigquery tables from projectId. TODO: Parallelize
func GetAllTables(bq *bigquery.Service, projectId string) ([]*bigquery.Table, error) {
	bqAPI, limiter := productionConfig(bq)
	return getAllTables(bqAPI, projectId, limiter)
}
