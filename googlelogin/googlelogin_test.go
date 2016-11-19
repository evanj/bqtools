package googlelogin

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/gorilla/securecookie"
)

type harness struct {
	securecookies *securecookie.SecureCookie
	auth          *Authenticator
	mux           *http.ServeMux
}

func (h *harness) sessionFromResponse(w *httptest.ResponseRecorder) *authState {
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		panic("invalid cookies")
	}
	session := &authState{}
	err := h.auth.securecookies.Decode(cookies[0].Name, cookies[0].Value, session)
	if err != nil {
		panic(err)
	}
	return session
}

func setupTestHarness() *harness {
	hashKey := make([]byte, cookieHashKeyLength)
	encryptionKey := make([]byte, cookieEncryptionKeyLength)
	securecookies := securecookie.New(hashKey, encryptionKey)
	mux := http.NewServeMux()
	auth, err := New("clientID", "clientSecret", "https://example.com/redirect", []string{"scope"},
		securecookies, "/noauth", mux)
	if err != nil {
		panic(err)
	}
	return &harness{securecookies, auth, mux}
}

func TestNew(t *testing.T) {
	h := setupTestHarness()
	// check that the mux handles the redirect
	r := httptest.NewRequest("GET", "/redirect", nil)
	_, pattern := h.mux.Handler(r)
	if pattern == "" {
		t.Fatal("redirect handler not registered")
	}
}

func TestCallback(t *testing.T) {
	// call Start to create a valid redirect; parse the redirect to be able to create a valid callback
	h := setupTestHarness()
	destination := "/some/path?query=foo"
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/start", nil)
	err := h.auth.Start(w, r, destination)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("redirect_uri") != "https://example.com/redirect" {
		t.Error(parsed.Query().Get("redirect_uri"))
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
		err := h.auth.handleCallbackError(w, r)
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
	h.auth.oauthConfig.Endpoint.TokenURL = ts.URL + "/token"

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
	err = h.securecookies.Decode(finalCookie.Name, finalCookie.Value, session)
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
	token, err := h.auth.GetToken(r)
	if err != nil {
		t.Error(err)
	}
	if token.Type() != "Bearer" {
		t.Error(token.Type(), token.TokenType, token)
	}
}

func TestToken(t *testing.T) {
	h := setupTestHarness()
	// no cookie
	token, err := h.auth.GetToken(httptest.NewRequest("GET", "/foo", nil))
	if err != ErrNotAuthenticated {
		t.Error(err, token)
	}
}

func TestHandleWithClient(t *testing.T) {
	h := setupTestHarness()
	var token *oauth2.Token
	handleFunc := func(w http.ResponseWriter, r *http.Request, t *oauth2.Token) {
		token = t
	}
	handler := h.auth.Handler(handleFunc)

	// request without cookies: redirected to noauth, with the full path encoded
	w := httptest.NewRecorder()
	const origPath = "/hello/world?query=string&foo=bar"
	r := httptest.NewRequest("GET", origPath, nil)
	handler.ServeHTTP(w, r)
	resp := w.Result()
	if resp.StatusCode != http.StatusFound {
		t.Error(resp.Status)
	}
	redir, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Error(err)
	}
	if redir.Path != "/noauth" && redir.Query().Get("path") != origPath {
		t.Error(redir)
	}
	if token != nil {
		t.Error(token)
	}

	// create a session with an expired token: redirected to Google
	session := &authState{Token: &oauth2.Token{AccessToken: "access", Expiry: time.Now().Add(-time.Hour)}}
	cookie, err := makeCookie(h.auth.securecookies, session)
	if err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", origPath, nil)
	r.AddCookie(cookie)
	handler.ServeHTTP(w, r)
	resp = w.Result()
	if resp.StatusCode != http.StatusFound {
		t.Error(resp.Status)
	}
	redir, err = url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Error(err)
	}
	if redir.Host != "accounts.google.com" {
		t.Error(redir)
	}
	if token != nil {
		t.Error(token)
	}
	// check that the session has the correct destination
	session = h.sessionFromResponse(w)
	if session.Destination != origPath {
		t.Error(session.Destination)
	}
}
