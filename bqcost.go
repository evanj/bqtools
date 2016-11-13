package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"

	"github.com/go-gorp/gorp"
	"github.com/gorilla/securecookie"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/oauth2"
	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/evanj/bqbackup/bqdb"
	"github.com/evanj/bqbackup/bqscrape"
	"github.com/evanj/bqbackup/googlelogin"
)

// TODO: Load these in a configuration file!
// client id and secret for Google OAuth
const clientID = "329377969161-82blev2kcn2fqhppq6ns78jh67718jvb.apps.googleusercontent.com"
const clientSecret = "uu9G8NxNLeWbRTgvPgNbG_fl"
const redirectURL = "http://localhost:8080/oauth2callback"

// See http://www.gorillatoolkit.org/pkg/securecookie
const cookieHashKeyLength = 64
const cookieEncryptionKeyLength = 32

// Secure cookies
var cookieHashKey = mustDecodeHex("7b78e1662b9c4451a1b778814d0ae766cb3bcc521f87d38d126cd66cb37fcd7684c7eea08141e04b6ce5540c9bcd10ffe136a6711b24505b8813b6acefd3cfe2")
var cookieEncryptionKey = mustDecodeHex("3e385efa8cf1038b57f05091803282f9d0c0505c182831e301111bd33db8c9fe")

func mustDecodeHex(hexString string) []byte {
	out, err := hex.DecodeString(hexString)
	if err != nil {
		panic(err)
	}
	return out
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	fmt.Fprintf(w, `<html><body>
	<p>To use the BigQuery cost tool, you will need to provide read-only access to BigQuery.</p>
	<p><a href="/start">Provide access to Google BigQuery</a></p>
	</body></html>`)
}

func handleNoAuth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, `<html><body>
	<p>Unauthenticated: That feature requires access to Google BigQuery</p>
	<p><a href="/start">Provide access to Google BigQuery</a></p>
	</body></html>`)
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	err := s.auth.Start(w, r, "/projects")
	if err != nil {
		log.Printf("bqcost: error starting googlelogin: %s", err.Error())
		http.Error(w, "authentication error", http.StatusInternalServerError)
	}
}

type server struct {
	auth         *googlelogin.Authenticator
	dbmap        *gorp.DbMap
	startLoading func(user *bqdb.User, projectID string) error
}

func (s *server) projectsHandler(w http.ResponseWriter, r *http.Request, token *oauth2.Token) {
	parts := strings.Split(r.URL.Path, "/")
	log.Printf("%s %s %d", r.URL.Path, parts, len(parts))
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	projectID := parts[2]
	if projectID == "" {
		log.Printf("%s = listProjects", r.URL.Path)
		client := s.auth.Client(context.TODO(), token)
		listProjects(w, r, client)
	} else {
		log.Printf("%s = projectIndex(%s)", r.URL.Path, projectID)
		err := s.projectIndex(w, r, token, projectID)
		if err != nil {
			log.Printf("projectIndex error %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

var listProjectsTemplate = template.Must(template.New("list").Parse(`<html><head>
<title>Big Query Projects</title>
</head>
<body>
<h1>Select a Project</h1>
<ul>
{{range .Projects}}
<li><a href="/projects/{{.Id}}">{{.FriendlyName}} ({{.Id}})</a></li>
{{end}}
</ul>
</body>
</html>`))

func listProjects(w http.ResponseWriter, r *http.Request, client *http.Client) {
	bq, err := bigquery.New(client)
	if err != nil {
		panic(err)
	}
	result, err := bq.Projects.List().Do()
	if err != nil {
		panic(err)
	}

	if result.NextPageToken != "" {
		panic("next page token")
	}
	err = listProjectsTemplate.Execute(w, result)
	if err != nil {
		panic(err)
	}
}

type storageUsage struct {
	Percent float64
	Bytes   int64
	ID      string
}

type projectIndexVars struct {
	FriendlyName   string
	ProjectID      string
	DatasetStorage []*storageUsage
	TableStorage   []*storageUsage
}

var projectIndexTemplate = template.Must(template.New("projectIndex").Parse(`<html><head>
<title>Project {{.FriendlyName}} ({{.ProjectID}})</title>
</head>
<body>
<h1>Datasets</h1>
<table>
<thead>
<tr><th>Percent Storage</th><th>Storage</th><th>ID</th></tr>
</thead>

<tbody>
{{range .DatasetStorage}}
<tr><td>{{printf "%.1f%%" .Percent}}</td><td>{{.Bytes}}</td><td>{{.ID}}</td></tr>
{{end}}
</tbody>
</table>

<h1>Tables</h1>
<table>
<thead>
<tr><th>Percent Storage</th><th>Storage</th><th>ID</th></tr>
</thead>

<tbody>
{{range .TableStorage}}
<tr><td>{{printf "%.1f%%" .Percent}}</td><td>{{.Bytes}}</td><td>{{.ID}}</td></tr>
{{end}}
</tbody>
</table>
</body>
</html>`))

func (s *server) projectIndex(w http.ResponseWriter, r *http.Request, token *oauth2.Token,
	projectID string) error {

	log.Printf("projectIndex %s", projectID)
	user, err := s.getUserOrStartLoading(token, projectID)
	if err != nil {
		if err == errIsLoading {
			w.Header().Set("Content-Type", "text/plain;charset=utf-8")
			w.Write([]byte("Loading data from BigQuery. Please reload."))
			return nil
		} else {
			return err
		}
	}

	pageVariables := &projectIndexVars{}
	_, err = s.dbmap.Select(&pageVariables.DatasetStorage, `SELECT DatasetID AS ID, SUM(NumBytes) AS Bytes FROM 'Table'
		WHERE UserID=? AND ProjectID=? GROUP BY ID`, user.ID, projectID)
	if err != nil {
		return err
	}
	_, err = s.dbmap.Select(&pageVariables.TableStorage, `SELECT DatasetID || '.' || TableID AS ID, NumBytes AS Bytes FROM 'Table'
		WHERE UserID=? AND ProjectID=?`, user.ID, projectID)
	if err != nil {
		return err
	}

	// total bytes, compute percentages
	totalBytes := int64(0)
	for _, d := range pageVariables.DatasetStorage {
		totalBytes += d.Bytes
	}
	percentageMultiplier := 1.0 / (float64(totalBytes) / 100.0)
	for _, d := range pageVariables.DatasetStorage {
		d.Percent = float64(d.Bytes) * percentageMultiplier
	}
	for _, t := range pageVariables.TableStorage {
		t.Percent = float64(t.Bytes) * percentageMultiplier
	}

	return projectIndexTemplate.Execute(w, pageVariables)
}

var errIsLoading = errors.New("loading data from bigquery")

// Returns a user or calls loader() to transactionally start loading. loader cannot block, but if
// it returns as error the user will not be inserted. If loader starts a goroutine, it should copy
// data from user to avoid data races.
func (s *server) getUserOrStartLoading(token *oauth2.Token, projectID string) (
	*bqdb.User, error) {

	txn, err := s.dbmap.Begin()
	if err != nil {
		return nil, err
	}
	// don't forget to rollback
	defer txn.Rollback()

	user, err := bqdb.GetUserByAccessToken(txn, token.AccessToken)
	if err != nil {
		return nil, err
	}
	if user == nil {
		log.Printf("bqcost: token %s creating new user", token.AccessToken)
		user = &bqdb.User{}
		user.AccessToken = token.AccessToken
		user.IsLoading = true
		// insert before calling loader so it can store the primary key
		// TODO: This probably should use one transaction to create the user and another to toggle
		// "IsLoading": The commit could fail causing user id to be re-used, or the token to be
		// assigned to a different user id
		err = txn.Insert(user)
		if err != nil {
			return nil, err
		}

		err = s.startLoading(user, projectID)
		if err != nil {
			return nil, err
		}

		// loading started successfully: commit the transaction
		err = txn.Commit()
		if err != nil {
			return nil, err
		}
		log.Printf("bqcost: token %s loading started", token.AccessToken)
		return nil, errIsLoading
	}

	log.Printf("bqcost: found user %v", user)
	if user.IsLoading {
		return nil, errIsLoading
	}
	if user.LoadingError != "" {
		return nil, errors.New(user.LoadingError)
	}
	return user, nil
}

func (s *server) finishLoading(userID int64, loadingErr error) error {
	txn, err := s.dbmap.Begin()
	if err != nil {
		return err
	}
	// don't forget to rollback
	defer txn.Rollback()

	user, err := bqdb.GetUserByID(txn, userID)
	if err != nil {
		return err
	}
	if user == nil {
		return fmt.Errorf("bqcost: finishLoading: user %d does not exist", userID)
	}
	if !user.IsLoading || user.LoadingError != "" {
		return fmt.Errorf("bqcost: finishLoading: user has already finished loading: %v", user)
	}
	user.IsLoading = false
	if loadingErr != nil {
		user.LoadingError = loadingErr.Error()
	}
	_, err = txn.Update(user)
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (s *server) startLocalhostLoader(user *bqdb.User, projectID string) error {
	// start a goroutine to start sync-ing data: copy args to avoid data races
	go s.localhostLoaderGoroutine(user.ID, user.AccessToken, projectID)
	return nil
}

func (s *server) localhostLoaderGoroutine(userID int64, accessToken string, projectID string) {
	log.Printf("bqcost: localhostLoaderGoroutine start user %d %s project %s",
		userID, accessToken, projectID)
	err := s.loadBigqueryData(userID, accessToken, projectID)
	if err != nil {
		log.Printf("bqcost: token %s loading error %s", accessToken, err.Error())
	}
	err = s.finishLoading(userID, err)
	if err != nil {
		log.Printf("bqcost: token %s error finishing loading: %s", accessToken, err.Error())
	}
	log.Printf("bqcost: localhostLoaderGoroutine end user %d %s project %s",
		userID, accessToken, projectID)
}

func (s *server) loadBigqueryData(userID int64, accessToken string, projectId string) error {
	client := s.auth.Client(context.TODO(), &oauth2.Token{AccessToken: accessToken})
	bq, err := bigquery.New(client)
	if err != nil {
		return err
	}

	tables, err := bqscrape.GetAllTables(bq, projectId)
	if err != nil {
		return err
	}

	return s.saveBigqueryTables(userID, tables)
}

func (s *server) saveBigqueryTables(userID int64, tables []*bigquery.Table) error {
	dbTables := make([]interface{}, len(tables))
	for i, table := range tables {
		if table.Type != bqscrape.TypeTable {
			log.Printf("bqcost: uid %d table %v ignoring table type %s",
				userID, table.TableReference, table.Type)
			continue
		}
		dbTable := &bqdb.Table{}
		dbTable.UserID = userID
		dbTable.ProjectID = table.TableReference.ProjectId
		dbTable.DatasetID = table.TableReference.DatasetId
		dbTable.TableID = table.TableReference.TableId
		dbTable.FriendlyName = table.FriendlyName
		dbTable.Description = table.Description
		dbTable.NumBytes = table.NumBytes
		dbTable.NumLongTermBytes = table.NumLongTermBytes
		dbTable.NumRows = int64(table.NumRows)

		dbTable.CreationTimeMs = table.CreationTime
		dbTable.LastModifiedTimeMs = int64(table.LastModifiedTime)

		if table.StreamingBuffer != nil {
			dbTable.StreamingEstimatedBytes = int64(table.StreamingBuffer.EstimatedBytes)
			dbTable.StreamingEstimatedRows = int64(table.StreamingBuffer.EstimatedRows)
		}
		dbTables[i] = dbTable
	}

	// let's do a massive insert: TODO: Does gorp actually execute this as batch?
	return s.dbmap.Insert(dbTables...)
}

func main() {
	securecookies := securecookie.New(cookieHashKey, cookieEncryptionKey)
	auth, err := googlelogin.New(clientID, clientSecret, redirectURL,
		[]string{bigquery.BigqueryScope + ".readonly"}, securecookies, "/noauth", http.DefaultServeMux)
	if err != nil {
		panic(err)
	}

	dbmap, err := bqdb.OpenAndCreateTablesIfNeeded("sqlite3", "test.sqlite", gorp.SqliteDialect{})
	if err != nil {
		panic(err)
	}

	s := &server{auth, dbmap, nil}
	// TODO: figure out a better way to customize this
	s.startLoading = s.startLocalhostLoader

	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/start", s.handleStart)
	http.HandleFunc("/noauth", handleNoAuth)

	http.Handle("/projects/", auth.Handler(s.projectsHandler))

	const hostport = "localhost:8080"
	fmt.Printf("listening on http://%s/\n", hostport)

	err = http.ListenAndServe(hostport, nil)
	if err != nil {
		panic(err)
	}
}
