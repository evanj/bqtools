package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/evanj/bqbackup/bqdb"
	"github.com/evanj/bqbackup/bqscrape"
	"github.com/go-gorp/gorp"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/oauth2"
	"google.golang.org/api/bigquery/v2"
)

func newTestDB() *gorp.DbMap {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
	err = bqdb.RegisterAndCreateTablesIfNeeded(dbmap)
	if err != nil {
		panic(err)
	}
	return dbmap
}

func TestGetUserOrStartLoading(t *testing.T) {
	dbmap := newTestDB()
	defer dbmap.Db.Close()

	var loaderUser *bqdb.User
	loaderProject := ""
	loader := func(u *bqdb.User, projectID string) error {
		// loader should be called with an initialized user so we can use the id
		if u.ID <= 0 {
			return fmt.Errorf("user must have id set: %v", u)
		}
		loaderUser = u
		loaderProject = projectID
		return nil
	}

	server := &server{nil, dbmap, loader}
	token := &oauth2.Token{AccessToken: "fake_access_token"}
	user, err := server.getUserOrStartLoading(token, "project")
	if user != nil || err != errIsLoading {
		t.Fatal(user, err)
	}
	if loaderUser.AccessToken != token.AccessToken || loaderUser.IsLoading != true || loaderUser.ID <= 0 {
		t.Error(user)
	}
	countUsers := func(token *oauth2.Token) int64 {
		count, err := dbmap.SelectInt("SELECT COUNT(*) FROM User WHERE AccessToken=?", token.AccessToken)
		if err != nil {
			panic(err)
		}
		return count
	}
	if countUsers(token) != 1 {
		t.Error(token)
	}
	loadedID := loaderUser.ID
	loaderUser = nil

	// calling it again with the same token should not call loader
	user, err = server.getUserOrStartLoading(token, "project")
	if user != nil || err != errIsLoading {
		t.Fatal(user, err)
	}
	if loaderUser != nil {
		t.Error(loaderUser)
	}

	// calling with a different token, where loader returns an error should not insert anything
	errLoading := errors.New("loading error")
	server.startLoading = func(u *bqdb.User, projectID string) error {
		loaderUser = u
		return errLoading
	}
	otherToken := &oauth2.Token{AccessToken: "other token"}
	user, err = server.getUserOrStartLoading(otherToken, "project")
	if user != nil || err != errLoading {
		t.Error(user, err)
	}
	if countUsers(otherToken) != 0 {
		t.Error(otherToken)
	}
	otherID := loaderUser.ID
	// finishing otherToken cannot work: was not inserted
	err = server.finishLoading(otherID, nil)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Error("expected does not exist error:", err)
	}

	// finish loading
	err = server.finishLoading(loadedID, errors.New("some err"))
	if err != nil {
		panic(err)
	}
	user, err = bqdb.GetUserByID(dbmap, loadedID)
	if err != nil {
		t.Error(err)
	}
	if user.IsLoading || user.LoadingError != "some err" {
		t.Error(user)
	}

	// finishing loading again fails
	err = server.finishLoading(loadedID, nil)
	if err == nil {
		t.Error(err)
	}

	// calling getUser again gets the error
	user, err = server.getUserOrStartLoading(token, "project")
	if err == nil || err.Error() != "some err" {
		t.Error("expected some err:", err)
	}
}

func TestSaveTables(t *testing.T) {
	dbmap := newTestDB()
	defer dbmap.Db.Close()

	s := &server{nil, dbmap, nil}

	tables := []*bigquery.Table{}
	for i := 0; i < 3; i++ {
		table := &bigquery.Table{
			Type: bqscrape.TypeTable,
			TableReference: &bigquery.TableReference{
				ProjectId: "p", DatasetId: "d", TableId: "table" + strconv.Itoa(i)},
		}
		tables = append(tables, table)
	}

	err := s.saveBigqueryTables(42, tables)
	if err != nil {
		t.Fatal(err)
	}
	count, err := dbmap.SelectInt("SELECT COUNT(*) FROM `Table`")
	if err != nil {
		t.Fatal(err)
	}
	if int(count) != len(tables) {
		t.Error(count, len(tables))
	}
}

func TestProjectReport(t *testing.T) {
	dbmap := newTestDB()
	defer dbmap.Db.Close()

	table := &bqdb.Table{}
	table.UserID = 1
	table.ProjectID = "p"
	table.DatasetID = "d1"
	table.TableID = "a"
	table.NumBytes = 1234
	err := dbmap.Insert(table)
	if err != nil {
		t.Fatal(err)
	}
	table.TableID = "b"
	table.NumBytes = 500000
	err = dbmap.Insert(table)
	if err != nil {
		t.Fatal(err)
	}
	const bigTableBytes = 1000000
	table.NumBytes = bigTableBytes
	table.DatasetID = "d2"
	const numExtraEntities = 50
	for i := 0; i < numExtraEntities; i++ {
		table.TableID = "table" + strconv.Itoa(i)
		err = dbmap.Insert(table)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < numExtraEntities; i++ {
		table.DatasetID = "extra" + strconv.Itoa(i)
		table.TableID = "table"
		table.NumBytes = 20
		err = dbmap.Insert(table)
		if err != nil {
			t.Fatal(err)
		}
	}
	const totalBytes = bigTableBytes*numExtraEntities + 500000 + 1234 + 20*numExtraEntities

	vars, err := queryProject(dbmap, 1, "p")
	if len(vars.DatasetStorage) != maxTopResults {
		t.Fatal(vars.DatasetStorage)
	}
	if vars.DatasetStorage[0].Bytes < vars.DatasetStorage[1].Bytes {
		t.Error("datasets must be sorted", vars.DatasetStorage[0], vars.DatasetStorage[1])
	}
	if len(vars.TableStorage) != maxTopResults {
		t.Fatal(vars.TableStorage)
	}
	if vars.TableStorage[0].Bytes < vars.TableStorage[1].Bytes {
		t.Error("datasets must be sorted", vars.TableStorage[0], vars.TableStorage[1])
	}
	const expectedPercentage = float64(bigTableBytes) * 100.0 / float64(totalBytes)
	if vars.TotalBytes != totalBytes {
		t.Errorf("expected total bytes %d got total bytes %d", totalBytes, vars.TotalBytes)
	}
	if vars.TableStorage[0].Percent(vars.TotalBytes) != expectedPercentage {
		t.Errorf("table %s percentage %f != expected %f",
			vars.TableStorage[0].ID, vars.TableStorage[0].Percent(vars.TotalBytes), expectedPercentage)
	}

	// TODO: Verify that rendering the template actually works
	u := &bqdb.User{ID: 1, AccessToken: "token"}
	err = dbmap.Insert(u)
	if err != nil {
		t.Fatal(err)
	}
	s := server{nil, dbmap, nil}
	w := httptest.NewRecorder()
	err = s.projectIndex(w, nil, &oauth2.Token{AccessToken: u.AccessToken}, "p")
	if err != nil {
		t.Fatal(err)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<td>d2.table0</td>") {
		t.Error("did not render template?")
		t.Error(body)
	}
}
