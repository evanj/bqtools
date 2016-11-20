package bqdb

import (
	"database/sql"
	"log"
	"reflect"
	"strings"

	gorp "github.com/go-gorp/gorp"
)

type User struct {
	ID          int64  `db:",primarykey,autoincrement"`
	AccessToken string `db:",notnull"`
}

type Project struct {
	UserID       int64  `db:",notnull"`
	ProjectID    string `db:",notnull"`
	FriendlyName string `db:",notnull"`

	IsLoading      bool   `db:",notnull"`
	LoadingPercent int    `db:",notnull"`
	LoadingMessage string `db:",notnull"`
	LoadingError   string `db:",notnull"`
}

// https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
type Table struct {
	UserID    int64
	ProjectID string
	DatasetID string
	TableID   string

	FriendlyName string `db:",notnull"`
	Description  string `db:",notnull"`

	NumBytes         int64 `db:",notnull"`
	NumLongTermBytes int64 `db:",notnull"`
	NumRows          int64 `db:",notnull"`

	CreationTimeMs     int64 `db:",notnull"`
	LastModifiedTimeMs int64 `db:",notnull"`

	StreamingEstimatedBytes int64 `db:",notnull"`
	StreamingEstimatedRows  int64 `db:",notnull"`
}

func OpenAndCreateTablesIfNeeded(driver string, path string, dialect gorp.Dialect) (*gorp.DbMap, error) {
	// set up the database
	db, err := sql.Open(driver, path)
	if err != nil {
		return nil, err
	}
	dbmap := &gorp.DbMap{Db: db, Dialect: dialect}
	err = RegisterAndCreateTablesIfNeeded(dbmap)
	if err != nil {
		return nil, err
	}
	return dbmap, nil
}

func RegisterAndCreateTablesIfNeeded(dbmap *gorp.DbMap) error {
	dbmap.AddTable(User{}).
		AddIndex("AccessTokenIndex", "", []string{"AccessToken"}).SetUnique(true)
	dbmap.AddTable(Project{}).SetKeys(false, "UserID", "ProjectID")
	dbmap.AddTable(Table{}).SetKeys(false, "UserID", "ProjectID", "DatasetID", "TableID")
	err := dbmap.CreateTablesIfNotExists()
	if err != nil {
		return err
	}
	err = dbmap.CreateIndex()
	// sqlite: index {{.Name}} already exists
	// mysql: Duplicate key name '{{.Name}}'
	if err != nil && (strings.Contains(err.Error(), "already exists") ||
		strings.Contains(err.Error(), "Duplicate key name")) {
		// assume this is probably index already exists
		log.Printf("bqdb: warning: ignoring error indexes already exist: %s", err.Error())
		return nil
	}
	return err
}

// Returns nil, nil if there is no such user (same as dbMap.Get()). TODO: Return err?
func GetUserByAccessToken(getter gorp.SqlExecutor, accessToken string) (*User, error) {
	user := &User{}
	err := getter.SelectOne(user, "SELECT * FROM User WHERE AccessToken=?", accessToken)
	if err != nil {
		user = nil
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return user, err
}

// Returns nil, nil if there is no such user (same as dbMap.Get()). TODO: Return err?
func GetUserByID(getter gorp.SqlExecutor, userID int64) (*User, error) {
	iface, err := getter.Get((*User)(nil), userID)
	if err != nil {
		return nil, err
	}
	var u *User
	if iface != nil {
		u = iface.(*User)
	}
	return u, nil
}

// Returns nil, nil if there is no such user (same as dbMap.Get()). TODO: Return err?
func GetProjectByID(getter gorp.SqlExecutor, userID int64, projectID string) (*Project, error) {
	iface, err := getter.Get((*Project)(nil), userID, projectID)
	if err != nil {
		return nil, err
	}
	var p *Project
	if iface != nil {
		p = iface.(*Project)
	}
	return p, nil
}

func QuotedTableForQuery(dbmap *gorp.DbMap, i interface{}) (string, error) {
	// TODO: cache the query?
	tableMap, err := dbmap.TableFor(reflect.TypeOf(i), false)
	if err != nil {
		return "", err
	}
	return dbmap.Dialect.QuotedTableForQuery(tableMap.SchemaName, tableMap.TableName), nil
}

// TODO: Remove this? projectQuery already calls QuotedTableForQuery
func QueryTotalTableBytes(dbmap *gorp.DbMap, userID int64, projectID string) (int64, error) {
	quotedTable, err := QuotedTableForQuery(dbmap, Table{})
	if err != nil {
		return 0, err
	}
	query := "SELECT SUM(NumBytes) FROM " + quotedTable + " WHERE UserID=? AND ProjectID=?"

	return dbmap.SelectInt(query, userID, projectID)
}
