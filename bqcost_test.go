package main

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/evanj/bqtools/bqdb"
	"github.com/evanj/bqtools/bqscrape"
	"github.com/go-gorp/gorp"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/oauth2"
	"google.golang.org/api/bigquery/v2"
)

func newTestDB() *gorp.DbMap {
	dbmap, err := bqdb.OpenAndCreateTablesIfNeeded("sqlite3", ":memory:", gorp.SqliteDialect{})
	if err != nil {
		panic(err)
	}
	return dbmap
}

func countUsers(dbmap *gorp.DbMap, token *oauth2.Token) int64 {
	count, err := dbmap.SelectInt("SELECT COUNT(*) FROM User WHERE AccessToken=?", token.AccessToken)
	if err != nil {
		panic(err)
	}
	return count
}

func TestGetUserOrStartLoading(t *testing.T) {
	dbmap := newTestDB()
	defer dbmap.Db.Close()

	var loaderUserID int64
	loaderProjectID := ""
	loader := func(userID int64, projectID string, accessToken string) error {
		// loader should be called with an initialized user so we can use the id
		if userID <= 0 {
			return fmt.Errorf("userid must be set: %d", userID)
		}
		loaderUserID = userID
		loaderProjectID = projectID
		return nil
	}

	// creates a new user: returns errIsLoading but also the user
	server := &server{nil, dbmap, loader}
	token := &oauth2.Token{AccessToken: "fake_access_token"}
	userID, project, err := server.getProjectOrStartLoading(token, "project")
	if userID <= 0 || project == nil || err != errIsLoading {
		t.Fatal(userID, project, err)
	}
	if loaderUserID != userID || project.IsLoading != true {
		t.Error(userID, project)
	}
	if countUsers(dbmap, token) != 1 {
		t.Error(token)
	}
	loadedID := loaderUserID
	loaderUserID = 0

	// calling it again with the same token should not call loader, but should return the project
	userID, project, err = server.getProjectOrStartLoading(token, "project")
	if userID <= 0 || project == nil || err != errIsLoading {
		t.Fatal(userID, project, err)
	}
	if loaderUserID != 0 {
		t.Error("calling twice should not have started loading:", loaderUserID)
	}

	// finish loading with an error
	err = server.finishLoading(loadedID, loaderProjectID, errors.New("some err"))
	if err != nil {
		panic(err)
	}
	project, err = bqdb.GetProjectByID(dbmap, loadedID, loaderProjectID)
	if err != nil {
		t.Error(err)
	}
	if project.IsLoading || project.LoadingError != "some err" {
		t.Error(project)
	}

	// finishing loading again fails
	err = server.finishLoading(loadedID, loaderProjectID, nil)
	if err == nil || !strings.Contains(err.Error(), "finished loading") {
		t.Error(err)
	}

	// calling getUser again gets the error
	userID, project, err = server.getProjectOrStartLoading(token, "project")
	if !(userID == 0 && project == nil && err != nil && err.Error() == "some err") {
		t.Error("expected some err:", userID, project, err)
	}

	// calling getProjectOrStartLoading with a different project causes that project to start
	userID, project, err = server.getProjectOrStartLoading(token, "project2")
	if !project.IsLoading || err != errIsLoading {
		t.Error(userID, project, err)
	}
}

func TestGetProjectOrStartLoadingError(t *testing.T) {
	dbmap := newTestDB()
	defer dbmap.Db.Close()

	var loaderUserID int64
	errLoading := errors.New("loading error")
	loader := func(userID int64, projectID string, accessToken string) error {
		loaderUserID = userID
		return errLoading
	}
	server := &server{nil, dbmap, loader}

	// when the loader returns an error, nothisg should be inserted
	otherToken := &oauth2.Token{AccessToken: "other token"}
	userID, project, err := server.getProjectOrStartLoading(otherToken, "project")
	if !(userID == 0 && project == nil && err == errLoading) {
		t.Error(userID, project, err)
	}
	if loaderUserID <= 0 {
		t.Error("expected loading to be called")
	}
	if countUsers(dbmap, otherToken) != 0 {
		t.Error(otherToken)
	}

	// finishing otherToken cannot work: was not inserted
	err = server.finishLoading(loaderUserID, "project", nil)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Error("expected does not exist error:", err)
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
	if err != nil {
		t.Fatal(err)
	}
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
	if vars.TableStorage[0].PercentValue(vars.TotalBytes) != expectedPercentage {
		t.Errorf("table %s percentage %f != expected %f",
			vars.TableStorage[0].ID, vars.TableStorage[0].PercentValue(vars.TotalBytes),
			expectedPercentage)
	}

	// TODO: Verify that rendering the template actually works
	u := &bqdb.User{ID: 1, AccessToken: "token"}
	p := &bqdb.Project{UserID: 1, ProjectID: table.ProjectID}
	err = dbmap.Insert(u, p)
	if err != nil {
		t.Fatal(err)
	}
	s := server{nil, dbmap, nil}
	w := httptest.NewRecorder()
	err = s.projectIndex(w, nil, &oauth2.Token{AccessToken: u.AccessToken}, p.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	body := w.Body.String()
	if !strings.Contains(body, ">d2.table0<") {
		t.Error("did not render template?")
		t.Error(body)
	}
}

func TestLoading(t *testing.T) {
	dbmap := newTestDB()
	defer dbmap.Db.Close()

	u := &bqdb.User{ID: 2}
	p := &bqdb.Project{UserID: u.ID, ProjectID: "project"}
	err := dbmap.Insert(u, p)
	if err != nil {
		t.Fatal(err)
	}

	// progress on a project that is not loading: don't update
	err = progressReport(dbmap, p.UserID, p.ProjectID, 55, "foo message")
	if err == nil || !strings.Contains(err.Error(), "not loading") {
		t.Error(err)
	}

	// progress on a project that does not exist: don't crash
	err = progressReport(dbmap, -5, p.ProjectID, 55, "foo message")
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Error(err)
	}

	// correct progress
	p.IsLoading = true
	_, err = dbmap.Update(p)
	if err != nil {
		t.Fatal(err)
	}
	err = progressReport(dbmap, p.UserID, p.ProjectID, 55, "foo message")
	if err != nil {
		t.Error(err)
	}

	// re-read the project
	p, err = bqdb.GetProjectByID(dbmap, p.UserID, p.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if !(p.LoadingPercent == 55 && p.LoadingMessage == "foo message") {
		t.Error(p)
	}

	// projectIndex outputs the data
	s := server{dbmap: dbmap}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/projects/"+p.ProjectID, nil)
	err = s.projectIndex(w, r, &oauth2.Token{AccessToken: "token"}, p.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(w.Body.String(), "foo message") {
		t.Error(w.Body.String())
	}
}

// func TestEmptyProject(t *testing.T) {
// 	t.Error("TODO: empty projects should work")
// }
