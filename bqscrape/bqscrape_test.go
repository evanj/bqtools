package bqscrape

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"google.golang.org/api/googleapi"

	"golang.org/x/time/rate"
	"google.golang.org/api/bigquery/v2"
)

const itemsPerPage = 2

type fakeBigQueryAPI struct {
	err           error
	datasetTables map[string][]string
}

func extractPageSlice(items []string, pageToken string) ([]string, string, error) {
	var err error
	index := 0
	if pageToken != "" {
		index, err = strconv.Atoi(pageToken)
		if err != nil {
			return nil, "", err
		}
	}

	upper := index + itemsPerPage
	if upper > len(items) {
		upper = len(items)
	}

	nextPageToken := ""
	if upper != len(items) {
		nextPageToken = strconv.Itoa(upper)
	}
	return items[index:upper], nextPageToken, nil
}

func (a *fakeBigQueryAPI) listDatasets(projectId string, pageToken string) (
	*bigquery.DatasetList, error) {
	if a.err != nil {
		return nil, a.err
	}

	// generate list of datasets
	datasets := []string{}
	for datasetID, _ := range a.datasetTables {
		datasets = append(datasets, datasetID)
	}
	sort.Strings(datasets)

	slice, nextPageToken, err := extractPageSlice(datasets, pageToken)
	if err != nil {
		return nil, err
	}

	result := &bigquery.DatasetList{}
	result.NextPageToken = nextPageToken
	for _, datasetID := range slice {
		ds := &bigquery.DatasetListDatasets{
			DatasetReference: &bigquery.DatasetReference{ProjectId: projectId, DatasetId: datasetID},
		}
		result.Datasets = append(result.Datasets, ds)
	}
	return result, nil
}

func (a *fakeBigQueryAPI) listTables(projectId string, datasetID string, pageToken string) (
	*bigquery.TableList, error) {

	tables := a.datasetTables[datasetID]
	slice, nextPageToken, err := extractPageSlice(tables, pageToken)
	if err != nil {
		return nil, err
	}

	result := &bigquery.TableList{}
	result.NextPageToken = nextPageToken
	for _, tableID := range slice {
		table := &bigquery.TableListTables{
			TableReference: &bigquery.TableReference{
				ProjectId: projectId, DatasetId: datasetID, TableId: tableID},
		}
		result.Tables = append(result.Tables, table)
	}
	return result, nil
}

func (a *fakeBigQueryAPI) getTable(projectId string, datasetId string, tableId string) (
	*bigquery.Table, error) {

	// TODO: check that the table "exists?"
	return &bigquery.Table{
		TableReference: &bigquery.TableReference{
			ProjectId: projectId, DatasetId: datasetId, TableId: tableId},
	}, nil
}

func TestListAllDatasets(t *testing.T) {
	fakeBQ := &fakeBigQueryAPI{}
	limiter := rate.NewLimiter(rate.Inf, 0)

	// check that listing a few pages works
	fakeBQ.datasetTables = map[string][]string{
		"ds0": []string{},
		"ds1": []string{},
		"ds2": []string{},
		"ds3": []string{},
		"ds4": []string{},
	}
	datasets, err := listAllDatasets(fakeBQ, "project", limiter)
	if len(datasets) != 5 || err != nil {
		t.Fatal(datasets, err)
	}
	if datasets[4].DatasetReference.DatasetId != "ds4" {
		t.Error(datasets[4].DatasetReference)
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
	if len(datasets) != 5 || err != nil {
		t.Error(datasets, err)
	}
	if end.Sub(start) < 2*time.Millisecond {
		t.Error("not rate limited:", end.Sub(start))
	}

	// check that we fail if we return too many datasets
	for i := 0; i < maxDatasets+1; i++ {
		dsId := "ds" + strconv.Itoa(i)
		fakeBQ.datasetTables[dsId] = []string{}
	}
	datasets, err = listAllDatasets(fakeBQ, "project", limiter)
	if datasets != nil || err == nil {
		t.Error(datasets, err)
	}
}

type progressReport struct {
	percent int
	message string
}
type FakeProgressReporter struct {
	progress []progressReport
}

func (p *FakeProgressReporter) Progress(percent int, message string) {
	p.progress = append(p.progress, progressReport{percent, message})
}

func TestGetAllTables(t *testing.T) {
	fakeBQ := &fakeBigQueryAPI{}
	fakeBQ.datasetTables = map[string][]string{
		"ds0": []string{"tableA", "tableB", "tableC"},
		"ds1": []string{"tableZ"},
	}
	limiter := rate.NewLimiter(rate.Inf, 0)

	progress := &FakeProgressReporter{}
	tables, err := getAllTables(fakeBQ, "project", limiter, progress)
	if err != nil {
		t.Fatal(err)
	}

	if len(tables) != 4 {
		t.Error(tables)
	}
	names := []string{}
	for _, t := range tables {
		names = append(names, t.TableReference.TableId)
	}
	if !reflect.DeepEqual(names, []string{"tableA", "tableB", "tableC", "tableZ"}) {
		t.Error(names)
	}

	expected := []progressReport{
		{0, "Listing tables..."},
		{10, "Reading table metadata: 0/4 tables"},
		{99, "Saving results..."},
	}
	if !reflect.DeepEqual(expected, progress.progress) {
		t.Error(progress.progress)
	}
}

func TestEstimateProgress(t *testing.T) {
	tests := []struct {
		listed int
		total  int
		report progressReport
	}{
		{0, 100, progressReport{10, "Reading table metadata: 0/100 tables"}},
		{50, 100, progressReport{54, "Reading table metadata: 50/100 tables"}},
		{100, 100, progressReport{99, "Reading table metadata: 100/100 tables"}},
	}

	for i, test := range tests {
		percent, message := estimateListTablesProgress(test.listed, test.total)
		report := progressReport{percent, message}
		if report != test.report {
			t.Errorf("%d: estimateListTablesProgress(%d, %d) = %v ; expected %v",
				i, test.listed, test.total, report, test.report)
		}
	}
}

func TestIsPermanentErr(t *testing.T) {
	// See https://cloud.google.com/bigquery/troubleshooting-errors
	retriableErrors := []error{
		&url.Error{Op: "GET", URL: "http://example.com/", Err: io.ErrUnexpectedEOF},
		&googleapi.Error{Code: http.StatusBadGateway, Message: "TODO: real error",
			Errors: []googleapi.ErrorItem{
				googleapi.ErrorItem{Reason: "backendError", Message: "TODO: real error"}}},

		// it is annoying that BigQuery re-uses HTTP status codes in this way
		&googleapi.Error{Code: http.StatusForbidden, Message: "TODO: get real message",
			Errors: []googleapi.ErrorItem{
				googleapi.ErrorItem{Reason: "rateLimitExceeded", Message: "TODO: get real message"}}},

		// have not seen error without items; but test it
		&googleapi.Error{Code: http.StatusForbidden, Message: "error without items"},
	}
	permanentErrors := []error{
		&googleapi.Error{Code: http.StatusForbidden, Message: "TODO: get real message",
			Errors: []googleapi.ErrorItem{
				googleapi.ErrorItem{Reason: "accessDenied", Message: "TODO: get real message"}}},

		// never seen an error with multiple items, but test it
		&googleapi.Error{Code: http.StatusForbidden, Message: "TODO: get real message",
			Errors: []googleapi.ErrorItem{
				googleapi.ErrorItem{Reason: "accessDenied", Message: "TODO: get real message"},
				googleapi.ErrorItem{Reason: "backendError", Message: "ignored"}}},
	}

	for i, err := range retriableErrors {
		if isPermanentErr(err) {
			t.Errorf("%d: temporary error incorrectly marked permanent; type: %v message: %s",
				i, reflect.TypeOf(err), err.Error())
		}
	}
	for i, err := range permanentErrors {
		if !isPermanentErr(err) {
			t.Errorf("%d: permanent error incorrectly marked temporary: %v message: %s",
				i, reflect.TypeOf(err), err.Error())
		}
	}
}

func TestRetry(t *testing.T) {
	permanentErr := &googleapi.Error{Code: http.StatusForbidden, Message: "TODO: get real message",
		Errors: []googleapi.ErrorItem{
			googleapi.ErrorItem{Reason: "accessDenied", Message: "TODO: get real message"}}}
	maybeTransientErr := errors.New("some unknown error")

	// permanent errors are not retried
	attempts := 0
	task := func() error {
		attempts += 1
		return permanentErr
	}
	err := retry(context.Background(), task)
	if err != permanentErr {
		t.Error("should return permanentErr:", err)
	}
	if attempts != 1 {
		t.Error("should not be retried")
	}

	// transient errors are retried until the time limit
	attempts = 0
	task = func() error {
		attempts += 1
		return maybeTransientErr
	}
	err = retry(context.Background(), task)
	if err != maybeTransientErr {
		t.Error("should return maybeTransientErr:", err)
	}
	if attempts <= 1 {
		t.Error("should be retried", attempts)
	}

	// cancelling causes it to not wait
	ctx, cancel := context.WithCancel(context.Background())
	attempts = 0
	task = func() error {
		attempts += 1
		cancel()
		return maybeTransientErr
	}
	err = retry(ctx, task)
	if err != context.Canceled {
		t.Error("should return cancelled:", err)
	}
	if attempts != 1 {
		t.Error("should not be retried", attempts)
	}

	// expired context also does not retry
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	attempts = 0
	task = func() error {
		attempts += 1
		return maybeTransientErr
	}
	err = retry(ctx, task)
	if err != context.DeadlineExceeded {
		t.Error("should return deadline exceeded:", err)
	}
	if attempts != 1 {
		t.Error("should not be retried", attempts)
	}
}
