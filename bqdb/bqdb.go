package bqdb

import (
	"database/sql"
	"log"
	"strings"

	gorp "github.com/go-gorp/gorp"
)

type User struct {
	ID             int64  `db:",primarykey,autoincrement"`
	AccessToken    string `db:",notnull"`
	IsLoading      bool   `db:",notnull"`
	LoadingPercent int    `db:",notnull"`
	LoadingMessage string `db:",notnull"`
	LoadingError   string `db:",notnull"`
}

// type Project struct {
// 	UserId       int64
// 	Id           string
// 	FriendlyName string `db:",notnull"`
// }

// type Dataset struct {
// 	UserId    int64
// 	ProjectId string
// 	Id        string
// }

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
	// dbmap.AddTable(Project{}).SetKeys(false, "UserId", "Id")
	// dbmap.AddTable(Dataset{}).SetKeys(false, "UserId", "ProjectId", "Id")
	dbmap.AddTable(Table{}).SetKeys(false, "UserID", "ProjectID", "DatasetID", "TableID")
	err := dbmap.CreateTablesIfNotExists()
	if err != nil {
		return err
	}
	err = dbmap.CreateIndex()
	// sqlite: index {{.Name}} already exists
	if err != nil && strings.Contains(err.Error(), "already exists") {
		// assume this is probably index already exists
		log.Printf("bqdb: warning: ignoring error indexes already exist: %s", err.Error())
		return nil
	}
	return err
}

type GorpGetter interface {
	Get(i interface{}, keys ...interface{}) (interface{}, error)
	SelectOne(i interface{}, query string, args ...interface{}) error
}

// Returns nil, nil if there is no such user
func GetUserByAccessToken(getter GorpGetter, accessToken string) (*User, error) {
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

// Returns nil, nil if there is no such user
func GetUserByID(getter GorpGetter, userID int64) (*User, error) {
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
