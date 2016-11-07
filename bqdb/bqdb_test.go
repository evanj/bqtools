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
}
