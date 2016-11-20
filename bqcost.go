package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"github.com/go-gorp/gorp"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/securecookie"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/evanj/bqbackup/bqdb"
	"github.com/evanj/bqbackup/bqscrape"
	"github.com/evanj/bqbackup/googlelogin"
	"github.com/evanj/bqbackup/templates"
)

const redirectPath = "/oauth2callback"
const productionHost = "https://bigquery-tools.appspot-preview.com"
const maxTopResults = 20

// For secure cookies. See http://www.gorillatoolkit.org/pkg/securecookie
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

	err := templates.Index(w)
	if err != nil {
		panic(err)
	}
}

func handleNoAuth(w http.ResponseWriter, r *http.Request) {
	// TODO: Make a pretty template
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
	startLoading func(userID int64, projectID string, accessToken string) error
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

func listProjects(w http.ResponseWriter, r *http.Request, client *http.Client) {
	bq, err := bigquery.New(client)
	if err != nil {
		panic(err)
	}
	result, err := bq.Projects.List().MaxResults(500).Do()
	if err != nil {
		panic(err)
	}

	if result.NextPageToken != "" {
		panic("projects next page token: not supported yet")
	}
	err = templates.SelectProject(w, result)
	if err != nil {
		panic(err)
	}
}

func queryProject(dbmap *gorp.DbMap, userID int64, projectID string) (*templates.ProjectData, error) {
	total, err := bqdb.QueryTotalTableBytes(dbmap, userID, projectID)
	if err != nil {
		return nil, err
	}
	quotedTable, err := bqdb.QuotedTableForQuery(dbmap, bqdb.Table{})
	if err != nil {
		return nil, err
	}

	// TODO: Set FriendlyName correctly
	data := &templates.ProjectData{ID: projectID, FriendlyName: projectID, TotalBytes: total}
	_, err = dbmap.Select(&data.DatasetStorage,
		"SELECT DatasetID AS ID, SUM(NumBytes) AS Bytes FROM "+quotedTable+
			" WHERE UserID=? AND ProjectID=? GROUP BY ID ORDER BY Bytes DESC LIMIT ?",
		userID, projectID, maxTopResults)
	if err != nil {
		return nil, err
	}

	ifaces, err := dbmap.Select((*bqdb.Table)(nil),
		"SELECT DatasetID, TableID, NumBytes FROM "+quotedTable+
			" WHERE UserID=? AND ProjectID=? ORDER BY NumBytes DESC LIMIT ?",
		userID, projectID, maxTopResults)
	if err != nil {
		return nil, err
	}
	data.TableStorage = make([]*templates.StorageUsage, len(ifaces))
	for i, iface := range ifaces {
		table := iface.(*bqdb.Table)
		id := table.DatasetID + "." + table.TableID
		data.TableStorage[i] = &templates.StorageUsage{ID: id, Bytes: table.NumBytes}
	}

	return data, nil
}

func (s *server) projectIndex(w http.ResponseWriter, r *http.Request, token *oauth2.Token,
	projectID string) error {

	log.Printf("projectIndex %s", projectID)
	userID, project, err := s.getProjectOrStartLoading(token, projectID)
	if err != nil {
		if err == errIsLoading {
			return templates.Loading(w, project.LoadingPercent, project.LoadingMessage)
		} else {
			return err
		}
	}

	pageVariables, err := queryProject(s.dbmap, userID, projectID)
	if err != nil {
		return err
	}
	return templates.Project(w, pageVariables)
}

// TODO: Remove: see comment below
var errIsLoading = errors.New("loading data from bigquery")

// Returns a userID, Project or calls loader() to transactionally start loading. loader cannot
// block, but if it returns as error the user will not be inserted. If loader starts a goroutine,
// it should copy data from user to avoid data races.
// TODO: This should not return errIsLoading; it should be the caller's responsibility to check
// if the user is loading
func (s *server) getProjectOrStartLoading(token *oauth2.Token, projectID string) (
	int64, *bqdb.Project, error) {

	txn, err := s.dbmap.Begin()
	if err != nil {
		return 0, nil, err
	}
	// don't forget to rollback
	defer txn.Rollback()

	user, err := bqdb.GetUserByAccessToken(txn, token.AccessToken)
	if err != nil {
		return 0, nil, err
	}
	if user == nil {
		// TODO: This probably should use one transaction to create the user and another to toggle
		// "IsLoading": The commit could fail causing user id to be re-used, or the token to be
		// assigned to a different user id
		log.Printf("bqcost: token %s creating new user", token.AccessToken)
		user = &bqdb.User{}
		user.AccessToken = token.AccessToken
		err = txn.Insert(user)
		if err != nil {
			return 0, nil, err
		}
	}
	log.Printf("bqcost: token %s found user %d", token.AccessToken, user.ID)

	project, err := bqdb.GetProjectByID(txn, user.ID, projectID)
	if err != nil {
		return 0, nil, err
	}
	if project == nil {
		log.Printf("bqcost: token %s user id %d creating new project %s",
			token.AccessToken, user.ID, projectID)
		project = &bqdb.Project{
			UserID:    user.ID,
			ProjectID: projectID,
			IsLoading: true,
		}
		err = txn.Insert(project)
		if err != nil {
			return 0, nil, err
		}

		err = s.startLoading(user.ID, projectID, user.AccessToken)
		if err != nil {
			return 0, nil, err
		}

		// loading started successfully: commit the transaction
		err = txn.Commit()
		if err != nil {
			return 0, nil, err
		}
		log.Printf("bqcost: token %s project %s loading started", token.AccessToken, projectID)
		return user.ID, project, errIsLoading
	}

	log.Printf("bqcost: found user %v", user)
	if project.IsLoading {
		return user.ID, project, errIsLoading
	}
	if project.LoadingError != "" {
		return 0, nil, errors.New(project.LoadingError)
	}
	return user.ID, project, nil
}

func (s *server) finishLoading(userID int64, projectID string, loadingErr error) error {
	txn, err := s.dbmap.Begin()
	if err != nil {
		return err
	}
	// don't forget to rollback
	defer txn.Rollback()

	project, err := bqdb.GetProjectByID(txn, userID, projectID)
	if err != nil {
		return err
	}
	if project == nil {
		return fmt.Errorf("bqcost: finishLoading: project %d %s does not exist", userID, projectID)
	}
	if !project.IsLoading || project.LoadingError != "" {
		return fmt.Errorf("bqcost: finishLoading: project has already finished loading: %v", project)
	}
	project.IsLoading = false
	if loadingErr != nil {
		project.LoadingError = loadingErr.Error()
	}
	_, err = txn.Update(project)
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (s *server) startLocalhostLoader(userID int64, projectID string, accessToken string) error {
	// start a goroutine to start sync-ing data: copy args to avoid data races
	go s.localhostLoaderGoroutine(userID, projectID, accessToken)
	return nil
}

func (s *server) localhostLoaderGoroutine(userID int64, projectID string, accessToken string) {
	log.Printf("bqcost: localhostLoaderGoroutine start user %d project %s", userID, projectID)
	err := s.loadBigqueryData(userID, projectID, accessToken)
	if err != nil {
		log.Printf("bqcost: token %s loading error %s", accessToken, err.Error())
	}
	err = s.finishLoading(userID, projectID, err)
	if err != nil {
		log.Printf("bqcost: token %s error finishing loading: %s", accessToken, err.Error())
	}
	log.Printf("bqcost: localhostLoaderGoroutine end user %d %s project %s",
		userID, accessToken, projectID)
}

func progressReport(dbmap *gorp.DbMap, userID int64, projectID string, percent int,
	message string) error {

	// super inefficient since we run this in a transaction
	txn, err := dbmap.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	// race: scraping is started in a goroutine, then the transaction is committed
	// by the time we read this, the user might not exist
	p, err := bqdb.GetProjectByID(txn, userID, projectID)
	if err != nil {
		return err
	}
	if p == nil {
		// TODO: log and return nil? this is pretty harmless?
		return fmt.Errorf("bqcost.progressReport: project %d %s does not exist; retry later",
			userID, projectID)
	}

	if !p.IsLoading {
		return fmt.Errorf("bqcost.progressReport: project %d %s is not loading", userID, projectID)
	}

	p.LoadingPercent = percent
	p.LoadingMessage = message
	_, err = txn.Update(p)
	if err != nil {
		return err
	}
	return txn.Commit()
}

type userProgressReporter struct {
	dbmap     *gorp.DbMap
	userID    int64
	projectID string
}

func (u *userProgressReporter) Progress(percent int, message string) {
	log.Printf("bqcost: progress report user %d: %d%% %s", u.userID, percent, message)
	err := progressReport(u.dbmap, u.userID, u.projectID, percent, message)
	if err != nil {
		log.Printf("bqcost: error in progress report: %s", err.Error())
	}
}

func (s *server) loadBigqueryData(userID int64, projectID string, accessToken string) error {
	client := s.auth.Client(context.TODO(), &oauth2.Token{AccessToken: accessToken})
	bq, err := bigquery.New(client)
	if err != nil {
		return err
	}

	progress := &userProgressReporter{s.dbmap, userID, projectID}
	tables, err := bqscrape.GetAllTables(bq, projectID, progress)
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
	sqlitePath := flag.String("sqlitePath", "", "If set, runs the server in localhost test mode")
	cloudSQLProxy := flag.Bool("cloudSQLProxy", false, "If set, runs in localhost mode conecting to cloud SQL")
	flag.Parse()

	listenHostPost := ":8080"
	redirectURL := productionHost + redirectPath
	dbDriver := "mysql"
	dbPath := "root@unix(/cloudsql/bigquery-tools:us-central1:bqcost-prod)/bqcost?interpolateParams=true"
	var dialect gorp.Dialect = gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}
	if *sqlitePath != "" || *cloudSQLProxy {
		log.Printf("starting in local test mode")
		listenHostPost = "localhost:8080"
		redirectURL = "http://" + listenHostPost + redirectPath
		if *sqlitePath != "" {
			dbDriver = "sqlite3"
			dbPath = *sqlitePath
			dialect = gorp.SqliteDialect{}
		} else {
			dbPath = "root@cloudsql(bigquery-tools:us-central1:bqcost-prod)/bqcost?interpolateParams=true"
		}
	} else {
		log.Printf("using production configuration")
	}

	securecookies := securecookie.New(cookieHashKey, cookieEncryptionKey)
	auth, err := googlelogin.New(googleOAuthClientID, googleOAuthClientSecret, redirectURL,
		[]string{bigquery.BigqueryScope + ".readonly"}, securecookies, "/noauth", http.DefaultServeMux)
	if err != nil {
		panic(err)
	}

	dbmap, err := bqdb.OpenAndCreateTablesIfNeeded(dbDriver, dbPath, dialect)
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

	fmt.Printf("listening on http://%s/\n", listenHostPost)
	err = http.ListenAndServe(listenHostPost, nil)
	if err != nil {
		panic(err)
	}
}
