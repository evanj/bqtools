package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/gorilla/csrf"
	"github.com/gorilla/securecookie"
)

// TODO: Load these in a configuration file!
// client id and secret for Google OAuth
const clientID = "329377969161-82blev2kcn2fqhppq6ns78jh67718jvb.apps.googleusercontent.com"
const clientSecret = "uu9G8NxNLeWbRTgvPgNbG_fl"

// Secure cookies
var cookieHashKey = mustDecodeHex("7b78e1662b9c4451a1b778814d0ae766cb3bcc521f87d38d126cd66cb37fcd7684c7eea08141e04b6ce5540c9bcd10ffe136a6711b24505b8813b6acefd3cfe2")
var cookieEncryptionKey = mustDecodeHex("3e385efa8cf1038b57f05091803282f9d0c0505c182831e301111bd33db8c9fe")
var csrfKey = mustDecodeHex("b2252c7fcea1537934bf4b45656d5b18e1780fa36825912c039071664fe3adbe")
var cookies = securecookie.New(cookieHashKey, cookieEncryptionKey)

const csrfFormName = "gorilla.csrf.Token"

const sessionCookieName = "session"
const oauthStateName = "state"

var bigQueryOAuthConfig = &oauth2.Config{
	ClientID:     clientID,
	ClientSecret: clientSecret,
	Endpoint:     google.Endpoint,
	Scopes:       []string{bigquery.CloudPlatformReadOnlyScope},
	RedirectURL:  "http://localhost:8080/oauth2callback",
}

type session struct {
	// token is gob serializable
	Token *oauth2.Token
	// unique id to identify sessions and validate oauth requests
	Id uint64
}

// data that is passed in the oauth "state" parameter: permits multiple requests
// concurrently to "work", e.g. when the token is expired and the user reloads all tabs.
// It is encrypted and maced just like cookies
type oauthState struct {
	// must match the session cookie
	SessionId   uint64
	Destination string
}

func setCookie(w http.ResponseWriter, value *session) error {
	encoded, err := cookies.Encode(sessionCookieName, value)
	if err != nil {
		return err
	}

	cookie := &http.Cookie{
		Name:     sessionCookieName,
		Value:    encoded,
		Path:     "/",
		HttpOnly: true,
	}
	http.SetCookie(w, cookie)
	return nil
}

// Returns the session for this user, or creates a new one and saves it.
func getOrCreateCookie(w http.ResponseWriter, r *http.Request) (*session, error) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		if err == http.ErrNoCookie {
			session := &session{nil, makeNonce()}
			err := setCookie(w, session)
			if err != nil {
				return nil, err
			}
			return session, nil
		}
		return nil, err
	}

	requestSession := &session{}
	err = cookies.Decode(sessionCookieName, cookie.Value, &requestSession)
	if err != nil {
		return nil, err
	}
	return requestSession, nil
}

func makeNonce() uint64 {
	nonceInt := uint64(0)
	for nonceInt == 0 {
		nonce := make([]byte, 8)
		_, err := rand.Read(nonce)
		if err != nil {
			panic(err)
		}
		nonceInt = binary.BigEndian.Uint64(nonce)
	}
	log.Printf("nonce 0x%08x", nonceInt)
	return nonceInt
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	fmt.Fprintf(w, `<html><body><form method="post" action="/oauth2start">
<p>To use BigQuery backup  <input type="submit" value="sign in with google"></p>
<input type="hidden" name="%s" value="%s">
</form></body></html>`, csrfFormName, csrf.Token(r))
}

func oauthRedirect(w http.ResponseWriter, r *http.Request, requestSession *session, destinationPath string) error {
	state := &oauthState{requestSession.Id, destinationPath}
	stateSerialized, err := cookies.Encode(oauthStateName, state)
	if err != nil {
		return err
	}
	log.Printf("oauth state param = %s", stateSerialized)

	// use "auto" to get no prompt on "refresh"
	url := bigQueryOAuthConfig.AuthCodeURL(stateSerialized, oauth2.AccessTypeOnline,
		oauth2.SetAuthURLParam("approval_prompt", "auto"))
	http.Redirect(w, r, url, http.StatusFound)
	return nil
}

func handleOauth2Start(w http.ResponseWriter, r *http.Request) {
	// Require POST (with CSRF protection) so users must take an explicit action on our site
	// See http://www.oauthsecurity.com/
	if r.Method != "POST" {
		http.Error(w, "Must use POST to start authentication", http.StatusMethodNotAllowed)
		return
	}
	log.Print("oauth2 start")

	// cookie the browser then redirect
	requestSession, err := getOrCreateCookie(w, r)
	if err != nil {
		panic(err)
	}

	err = oauthRedirect(w, r, requestSession, "/projects")
}

func handleOauth2Callback(w http.ResponseWriter, r *http.Request) {
	log.Print("oauth2 callback")

	errorString := r.FormValue("error")
	if errorString != "" {
		log.Println("oauth2 error", errorString)
		http.Error(w, "Authentication error: must grant access to BigQuery: "+errorString, http.StatusInternalServerError)
		return
	}

	requestSession, err := getOrCreateCookie(w, r)
	if err != nil {
		log.Print("invalid cookie?", err.Error())
		http.Error(w, "Authentication error try again", http.StatusInternalServerError)
		return
	}

	stateSerialized := r.FormValue("state")
	state := &oauthState{}
	err = cookies.Decode(oauthStateName, stateSerialized, state)
	if err != nil {
		log.Println("state failed to decode", err)
		http.Error(w, "Authentication error try again", http.StatusInternalServerError)
		return
	}
	if state.SessionId != requestSession.Id {
		log.Println("mismatched session ids", state.SessionId, requestSession.Id)
		http.Error(w, "Authentication error try again", http.StatusInternalServerError)
		return
	}

	code := r.FormValue("code")
	if code == "" {
		log.Print("missing code?")
		http.Error(w, "Authentication error try again", http.StatusInternalServerError)
		return
	}

	// get the token for the code
	ctx := context.Background()
	token, err := bigQueryOAuthConfig.Exchange(ctx, code)
	if err != nil {
		log.Println("error exchanging code:", err)
		http.Error(w, "Authentication error try again", http.StatusInternalServerError)
		return
	}

	// save the token in the session
	requestSession.Token = token
	setCookie(w, requestSession)
	log.Println("oauth successful redirecting to", state.Destination)
	http.Redirect(w, r, state.Destination, http.StatusFound)
}

func listProjects(w http.ResponseWriter, r *http.Request) {
	requestSession, err := getOrCreateCookie(w, r)
	if requestSession.Token == nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	if !requestSession.Token.Valid() {
		log.Println("detected an invalid token; attempting to re-authenticate")
		oauthRedirect(w, r, requestSession, "/projects")
		return
	}

	ctx := context.Background()
	oauthClient := bigQueryOAuthConfig.Client(ctx, requestSession.Token)
	bq, err := bigquery.New(oauthClient)
	if err != nil {
		panic(err)
	}
	result, err := bq.Projects.List().Do()
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(w, "<html><body>")
	if result.NextPageToken != "" {
		panic("next page token")
	}
	for _, project := range result.Projects {
		fmt.Fprintf(w, "<p>%s %s</p>\n", project.FriendlyName, project.Id)
	}
	fmt.Fprintf(w, "wtf %s", requestSession.Token.Expiry.String())
	fmt.Fprintf(w, "</body></html>")
}

func handleProject(w http.ResponseWriter, r *http.Request) {

	requestSession, err := getOrCreateCookie(w, r)
	if requestSession.Token == nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	if !requestSession.Token.Valid() {
		log.Println("detected an invalid token; attempting to re-authenticate")
		oauthRedirect(w, r, requestSession, "/projects")
		return
	}

	ctx := context.Background()
	oauthClient := bigQueryOAuthConfig.Client(ctx, requestSession.Token)
	bq, err := bigquery.New(oauthClient)
	if err != nil {
		panic(err)
	}
	result, err := bq.Projects.List().Do()
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(w, "<html><body>")
	if result.NextPageToken != "" {
		panic("next page token")
	}
	for _, project := range result.Projects {
		fmt.Fprintf(w, "<p>%s %s</p>\n", project.FriendlyName, project.Id)
	}
	fmt.Fprintf(w, "wtf %s", requestSession.Token.Expiry.String())
	fmt.Fprintf(w, "</body></html>")
}

func mustDecodeHex(hexString string) []byte {
	out, err := hex.DecodeString(hexString)
	if err != nil {
		panic(err)
	}
	return out
}

// Creates a
func loggedInHandler(fn http.HandleFunc) http.HandleFunc {
  w http.ResponseWriter, r *http.Request

}

func main() {
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/oauth2start", handleOauth2Start)
	http.HandleFunc("/oauth2callback", handleOauth2Callback)
	http.HandleFunc("/projects", listProjects)
	http.HandleFunc("/project/", handleProject)

	const hostport = "localhost:8080"
	fmt.Printf("listening on http://%s/\n", hostport)

	// disable secure: we are serving over http for localhost
	csrfProtection := csrf.Protect(csrfKey, csrf.Secure(false))
	err := http.ListenAndServe(hostport, csrfProtection(http.DefaultServeMux))
	if err != nil {
		panic(err)
	}
}
