package bqscrape_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/evanj/bqbackup/bqscrape"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

func newDefaultBQ() *bigquery.Service {
	client, err := google.DefaultClient(context.Background(), bigquery.BigqueryScope+".readonly")
	if err != nil {
		panic(err)
	}
	bq, err := bigquery.New(client)
	if err != nil {
		panic(err)
	}
	return bq
}

func TestListAllTables(t *testing.T) {
	bq := newDefaultBQ()
	tables, err := bqscrape.ListAllTables(bq, "bigquery-tools")
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	tableIDs := []string{}
	for _, table := range tables {
		ids = append(ids, table.Id)
		tableIDs = append(tableIDs, table.TableReference.TableId)
	}
	if !reflect.DeepEqual(ids, []string{"bigquery-tools:github.event_sample"}) {
		t.Error(ids)
	}
	if !reflect.DeepEqual(tableIDs, []string{"event_sample"}) {
		t.Error(tableIDs)
	}
}

func TestGetAllTables(t *testing.T) {
	bq := newDefaultBQ()
	tables, err := bqscrape.GetAllTables(bq, "bigquery-tools", nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	tableIDs := []string{}
	for _, table := range tables {
		ids = append(ids, table.Id)
		tableIDs = append(tableIDs, table.TableReference.TableId)
		if table.NumBytes <= 0 {
			t.Error("table has no bytes?", table.Id, table.NumBytes)
		}
	}
	if !reflect.DeepEqual(ids, []string{"bigquery-tools:github.event_sample"}) {
		t.Error(ids)
	}
	if !reflect.DeepEqual(tableIDs, []string{"event_sample"}) {
		t.Error(tableIDs)
	}
}
