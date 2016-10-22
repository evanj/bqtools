package googlelogin

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/securecookie"
)

// func TestSessionStoreInvalid(t *testing.T) {
// 	hashKey := make([]byte, cookieHashKeyLength)
// 	encryptionKey := make([]byte, cookieEncryptionKeyLength)
// 	sessions := newSessionStore(hashKey, encryptionKey)

// 	sessionRequest := func(c *http.Cookie) (*httptest.ResponseRecorder, *session) {
// 		w := httptest.NewRecorder()
// 		r := httptest.NewRequest("GET", "/start", nil)
// 		if c != nil {
// 			r.AddCookie(c)
// 		}
// 		return w, sessions.get(w, r)
// 	}

// 	w, session := sessionRequest(nil)
// 	if !(session.Token == nil && session.Destination == "") {
// 		t.Error(session)
// 	}
// 	if w.Header().Get("Set-Cookie") != "" {
// 		t.Error(w.Header().Get("Set-Cookie"))
// 	}

// 	w, session = sessionRequest(&http.Cookie{Name: cookieName, Raw: "hello"})
// 	if !(session.Token == nil && session.Destination == "") {
// 		t.Error(session)
// 	}
// 	if w.Header().Get("Set-Cookie") != "" {
// 		t.Error(w.Header().Get("Set-Cookie"))
// 	}
// }

func TestCallback(t *testing.T) {
	// call Start to create a valid redirect; parse the redirect to be able to create a valid callback
	hashKey := make([]byte, cookieHashKeyLength)
	encryptionKey := make([]byte, cookieEncryptionKeyLength)
	securecookies := securecookie.New(hashKey, encryptionKey)
	auth := New("clientID", "clientSecret", "http://example.com/redirect", []string{"scope"},
		securecookies)

	destination := "/some/path?query=foo"
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/start", nil)
	err := auth.Start(w, r, destination)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	stateSerialized := parsed.Query().Get("state")
	if len(stateSerialized) == 0 {
		t.Fatal(stateSerialized)
	}
	// cookieHeader := w.Header().Get("Set-Cookie")
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal(cookies)
	}
	validCookie := cookies[0]

	doCallback := func(values map[string]string, cookie *http.Cookie) (*httptest.ResponseRecorder, error) {
		params := url.Values{}
		for k, v := range values {
			params.Set(k, v)
		}

		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/callback?"+params.Encode(), nil)
		if cookie != nil {
			r.Header.Set("Cookie", cookie.String())
		}
		err := auth.HandleCallback(w, r)
		return w, err
	}

	hasExpiredCookie := func(w *httptest.ResponseRecorder) bool {
		header := w.Header().Get("Set-Cookie")
		return strings.Contains(header, "Expires=Thu, 01 Jan 1970 00:00:01 GMT")
	}

	zeroState := base64.RawURLEncoding.EncodeToString(make([]byte, stateLength))
	errorValues := []map[string]string{
		// no parameters
		nil,
		map[string]string{"code": "", "state": ""},

		// oauth2 error from Google
		map[string]string{"error": "access_denied"},

		// state does not match cookie
		map[string]string{"code": "some_code", "state": zeroState},
	}

	for _, values := range errorValues {
		// test both with the cookie and without
		w, err := doCallback(values, nil)
		if err == nil {
			t.Error(err)
		}
		if !hasExpiredCookie(w) {
			t.Error(w)
		}
		w, err = doCallback(values, validCookie)
		if err == nil {
			t.Error(err)
		}
		if !hasExpiredCookie(w) {
			t.Error(w)
		}
	}

	// successful request: start a server to exchange the code; stolen from oauth2_test.go
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token": "90d", "scope": "user", "token_type": "bearer", "expires": 100}`))
	}))
	defer ts.Close()
	auth.oauthConfig.Endpoint.TokenURL = ts.URL + "/token"

	w, err = doCallback(map[string]string{"state": stateSerialized, "code": "code"}, validCookie)
	if err != nil {
		t.Error(err)
	}
	result := w.Result()
	if result.StatusCode != http.StatusFound {
		t.Error(result.Status)
	}
	if result.Header.Get("Location") != destination {
		t.Error(result.Header.Get("Location"))
	}
	if len(result.Cookies()) != 1 {
		t.Fatal(result.Cookies())
	}
	finalCookie := result.Cookies()[0]
	session := &authState{}
	err = securecookies.Decode(finalCookie.Name, finalCookie.Value, session)
	if err != nil {
		t.Error(err)
	}
	if session.State != nil {
		t.Error(session.State)
	}
	if session.Destination != "" {
		t.Error(session.Destination)
	}
	if session.Token == nil {
		t.Error(session.Token)
	}

	r = httptest.NewRequest("GET", "/foo", nil)
	r.Header.Set("Cookie", finalCookie.String())
	token, err := auth.GetToken(r)
	if err != nil {
		t.Error(err)
	}
	if token.Type() != "Bearer" {
		t.Error(token.Type(), token.TokenType, token)
	}
}

func TestToken(t *testing.T) {
	// call Start to create a valid redirect; parse the redirect to be able to create a valid callback
	hashKey := make([]byte, cookieHashKeyLength)
	encryptionKey := make([]byte, cookieEncryptionKeyLength)
	securecookies := securecookie.New(hashKey, encryptionKey)
	auth := New("clientID", "clientSecret", "http://example.com/redirect", []string{"scope"},
		securecookies)

	// no cookie
	token, err := auth.GetToken(httptest.NewRequest("GET", "/foo", nil))
	if err != ErrNotAuthenticated {
		t.Error(err, token)
	}
}

// func TestOAuth2StartExpiredCookie(t *testing.T) {
// 	hashKey := make([]byte, cookieHashKeyLength)
// 	encrytionKey := make([]byte, cookieEncryptionKeyLength)
// 	sessions := newSessionStore(hashKey, encryptionKey)

// 	// get with no cookies: should set the cookie
// 	w := httptest.NewRecorder()
// 	r := httptest.NewRequest("GET", "/start", nil)
// 	session, err := sessions.getOrCreate(w, r)
// 	if err != nil {
// 		panic(err)
// 	}
// }
