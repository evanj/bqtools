package googlelogin

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/gorilla/securecookie"
)

// See http://www.gorillatoolkit.org/pkg/securecookie
const cookieHashKeyLength = 64
const cookieEncryptionKeyLength = 32
const stateLength = 32
const cookieName = "googlelogin"

// TODO: Configurable? E.g. only keep a session cookie?
// TODO: expiration of securecookie should match this§q;
const cookieExpiration = 24 * 30 * time.Hour

// Either there is no saved token, or the cookie has expired.
var ErrNotAuthenticated = errors.New("googlelogin: not authenticated")
var ErrTokenExpired = errors.New("googlelogin: oauth2 token expired")

// If you request email or profile scopes, you will get an id_token in the response with
// details about the authenticated user. Otherwise, you just get an opaque token that you cannot
// connect to other tokens in the future.
// https://developers.google.com/identity/protocols/googlescopes#oauth2v2
// "Know who you are on Google" and "View your email address"
// hitting tokeninfo gives you scopes https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/plus.me
// const UserEmailScope = "email"
// "View your basic profile info"
// tokeninfo does not include email and has scope: https://www.googleapis.com/auth/userinfo.profile
// const UserProfileScope = "profile"

// Note: If you include "localhost" in the redirect_uri, Google may tell the user that you will "have offline access"
// http://stackoverflow.com/a/31242454/413438

// Authenticator obtains access tokens from Google on behalf of an end user web browser.
type Authenticator struct {
	oauthConfig   oauth2.Config
	securecookies *securecookie.SecureCookie
}

// New creates a new Authenticator for authenticating users. The clientID, clientSecret, and
// redirectURL must registered with Google. The scopes are
func New(clientID string, clientSecret string, redirectURL string,
	scopes []string, securecookies *securecookie.SecureCookie) *Authenticator {

	// TODO: Validate parameters
	return &Authenticator{
		oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Endpoint:     google.Endpoint,
			Scopes:       scopes,
			RedirectURL:  "http://localhost:8080/oauth2callback",
		},
		securecookies}
}

// Stores the user's Google OAuth access token and/or the state for an oauth login
type authState struct {
	// Token is gob serializable
	Token *oauth2.Token
	// unique state to validate oauth requests
	State []byte
	// destination path to redirect to after the authentication is complete
	Destination string
}

// Returns the current session, or a new zero session.
func (a *Authenticator) getSession(r *http.Request) *authState {
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		if err != http.ErrNoCookie {
			// should be the only kind of error
			log.Printf("googlelogin: error: ignoring expected error getting cookie: %s", err.Error())
		}
		// no session: return an empty session
		return &authState{}
	}

	session := &authState{}
	err = a.securecookies.Decode(cookie.Name, cookie.Value, session)
	if err != nil {
		log.Printf("googlelogin: error: ignoring invalid session cookie: %s", err.Error())
		return &authState{}
	}
	return session
}

func (a *Authenticator) saveSession(w http.ResponseWriter, session *authState) error {
	serialized, err := a.securecookies.Encode(cookieName, session)
	if err != nil {
		return err
	}
	cookie := &http.Cookie{
		Name:     cookieName,
		Path:     "/",
		Expires:  time.Now().Add(cookieExpiration),
		HttpOnly: true,
		Value:    serialized,
		// TODO: Set this based on an option
		// Secure:   true,
	}
	http.SetCookie(w, cookie)
	return nil
}

// Deletes the session cookie setting it to an expired empty value.
func deleteSession(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: cookieName,
		Path: "/",
		// expires must be non-zero to get output
		Expires:  time.Unix(1, 0),
		HttpOnly: true,
	})
}

// Redirects the browser to obtain a new token from Google.
// TODO: Ensure that this request was CSRF protected? http://www.oauthsecurity.com/
// TODO: Verify that this request was a POST?
func (a *Authenticator) Start(w http.ResponseWriter, r *http.Request, destinationPath string) error {

	if destinationPath[0] != '/' {
		return fmt.Errorf("googlelogin: destinationPath must be absolute")
	}

	// generate state to prevent CSRF: https://tools.ietf.org/html/rfc6749#section-10.12
	state, err := makeState()
	if err != nil {
		return err
	}
	session := &authState{nil, state, destinationPath}
	err = a.saveSession(w, session)
	if err != nil {
		return err
	}

	stateSerialized := base64.RawURLEncoding.EncodeToString(session.State)
	log.Printf("oauth state param = %s", stateSerialized)

	// AccessTypeOnline only gives us an access token, not a refresh token (lower security risk)
	// use "auto" to get no prompt on "refresh"
	url := a.oauthConfig.AuthCodeURL(stateSerialized, oauth2.AccessTypeOnline,
		oauth2.SetAuthURLParam("approval_prompt", "auto"))
	http.Redirect(w, r, url, http.StatusFound)
	return nil
}

func (a *Authenticator) HandleCallback(w http.ResponseWriter, r *http.Request) error {
	log.Println("oauth2 callback", r.Method, r.URL.String())

	errorString := r.FormValue("error")
	if errorString != "" {
		log.Printf("googlelogin: callback error: %s", errorString)
		// possible errors: https://tools.ietf.org/html/rfc6749#section-4.2.2.1
		deleteSession(w)
		return fmt.Errorf("googlelogin: oauth error response: %s", errorString)
	}

	stateString := r.FormValue("state")
	state, err := base64.RawURLEncoding.DecodeString(stateString)
	if err != nil || len(state) == 0 {
		log.Printf("googlelogin: invalid state '%s' err %v", stateString, err)
		deleteSession(w)
		return errors.New("googlelogin: invalid state parameter")
	}

	code := r.FormValue("code")
	if len(code) == 0 {
		log.Printf("googlelogin: missing code parameter")
		deleteSession(w)
		return errors.New("googlelogin: missing code parameter")
	}

	// on error the zero session will fail to match the incoming state
	session := a.getSession(r)
	if !bytes.Equal(state, session.State) {
		log.Printf("googlelogin: invalid session cookie state len %d", len(session.State))
		deleteSession(w)
		return errors.New("googlelogin: invalid session cookie")
	}
	if len(session.Destination) == 0 {
		log.Printf("googlelogin: invalid session cookie no destination")
		deleteSession(w)
		return errors.New("googlelogin: invalid session cookie no destination")
	}
	destination := session.Destination

	// things look like they might be valid! Let's get the token
	ctx := context.Background()
	token, err := a.oauthConfig.Exchange(ctx, code)
	if err != nil {
		log.Printf("googlelogin: error exchanging code %s", err.Error())
		deleteSession(w)
		return fmt.Errorf("googlelogin: error exchanging code %s", err.Error())
	}
	// TODO: If we requested email or profile the may contain .Extra("id_token") but it is not
	// serialized via gob. Read it and save it seperately?

	// save the token in the session, clear all temp variables
	session = &authState{Token: token}
	err = a.saveSession(w, session)
	if err != nil {
		log.Printf("googlelogin: error saving session cookie: %s", err.Error())
		deleteSession(w)
		return fmt.Errorf("googlelogin: error saving session cookie: %s", err.Error())
	}

	// it worked! redirect to the final destination
	log.Printf("googlelogin: oauth successful redirecting to %s", destination)
	http.Redirect(w, r, destination, http.StatusFound)
	return nil
}

// Returns the oauth2.Token corresponding to this request.
func (a *Authenticator) GetToken(r *http.Request) (*oauth2.Token, error) {
	session := a.getSession(r)
	if session.Token == nil {
		return nil, ErrNotAuthenticated
	}
	return session.Token, nil
}

// see https://tools.ietf.org/html/rfc6749#section-10.12
func makeState() ([]byte, error) {
	state := make([]byte, stateLength)
	_, err := rand.Read(state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

// Makes a request to Google's TokenInfo endpoint and returns the payload. Useful for verifying
// that a token is still valid. If the user explicitly revokes it, Google returns an HTTP error
// such as 400 Bad Request with additional details in the body (e.g "Invalid Value")
func GetTokenInfo(token *oauth2.Token) (string, error) {
	// https://developers.google.com/identity/sign-in/web/backend-auth#calling-the-tokeninfo-endpoint
	// https://developers.google.com/identity/protocols/OAuth2UserAgent#validatetoken
	// The endpoint takes either an access_token or id_token.
	data := url.Values{"access_token": []string{token.AccessToken}}
	resp, err := http.Post("https://www.googleapis.com/oauth2/v3/tokeninfo", "application/x-www-form-urlencoded",
		strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	err2 := resp.Body.Close()
	if err != nil {
		return "", err
	}
	if err2 != nil {
		return "", err2
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("googlelogin: tokeninfo error: %s %s", resp.Status, string(body))
	}
	return string(body), nil
}
