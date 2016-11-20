package bqdb

import (
	"reflect"
	"testing"

	"github.com/go-gorp/gorp"
	_ "github.com/mattn/go-sqlite3"
)

func TestRegister(t *testing.T) {
	// set up the database
	dbmap, err := OpenAndCreateTablesIfNeeded("sqlite3", ":memory:", gorp.SqliteDialect{})
	if err != nil {
		t.Fatal(err)
	}
	defer dbmap.Db.Close()
	user := &User{}
	user.AccessToken = "foo"
	err = dbmap.Insert(user)
	if err != nil {
		t.Error(err)
	}
	if user.ID <= 0 {
		t.Error(user)
	}

	// get the user: it should be a different object but be equal
	u2, err := GetUserByID(dbmap, user.ID)
	if err != nil {
		t.Error(err)
	}
	if u2 == user {
		t.Errorf("pointers should not be equal %v %v", u2, user)
	}
	if !reflect.DeepEqual(u2, user) {
		t.Error(u2, user)
	}

	// cannot insert duplicate access tokens
	u2 = &User{}
	u2.AccessToken = "foo"
	err = dbmap.Insert(u2)
	if err == nil {
		t.Error(err)
	}

	u2, err = GetUserByAccessToken(dbmap, "foo")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(u2, user) {
		t.Error(u2, user)
	}

	// query for a not found id
	u2, err = GetUserByID(dbmap, -1)
	if !(u2 == nil && err == nil) {
		t.Error(u2, err)
	}
	u2, err = GetUserByAccessToken(dbmap, "does not exist")
	if !(u2 == nil && err == nil) {
		t.Error(u2, err)
	}
}

func TestQuerySum(t *testing.T) {
	// set up the database
	dbmap, err := OpenAndCreateTablesIfNeeded("sqlite3", ":memory:", gorp.SqliteDialect{})
	if err != nil {
		t.Fatal(err)
	}
	defer dbmap.Db.Close()

	// does not exist: should return an error
	count, err := QueryTotalTableBytes(dbmap, 42, "project")
	if err == nil {
		t.Error(count, err)
	}

	table := &Table{}
	table.UserID = 42
	table.ProjectID = "project"
	table.TableID = "a"
	table.NumBytes = 5
	err = dbmap.Insert(table)
	if err != nil {
		t.Fatal(err)
	}
	table.TableID = "b"
	table.NumBytes = 7
	err = dbmap.Insert(table)
	if err != nil {
		t.Fatal(err)
	}

	count, err = QueryTotalTableBytes(dbmap, 42, "project")
	if err != nil {
		t.Fatal(err)
	}
	if count != 12 {
		t.Error(count)
	}
}

func TestQuotedTable(t *testing.T) {
	// set up the database
	dbmap, err := OpenAndCreateTablesIfNeeded("sqlite3", ":memory:", gorp.SqliteDialect{})
	if err != nil {
		t.Fatal(err)
	}
	defer dbmap.Db.Close()

	table, err := QuotedTableForQuery(dbmap, Table{})
	if err != nil {
		t.Fatal(err)
	}
	if table != `"Table"` {
		t.Error(table)
	}

	dbmap.Dialect = gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}
	table, err = QuotedTableForQuery(dbmap, Table{})
	if err != nil {
		t.Fatal(err)
	}
	if table != "`Table`" {
		t.Error(table)
	}
}
