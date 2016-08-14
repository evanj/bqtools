package main

import (
	"crypto/rand"
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

const cookieName = "state"

var bigQueryOAuthConfig = &oauth2.Config{
	ClientID:     clientID,
	ClientSecret: clientSecret,
	Endpoint:     google.Endpoint,
	Scopes:       []string{bigquery.CloudPlatformReadOnlyScope},
	RedirectURL:  "http://localhost:8080/oauth2callback",
}

type session struct {
	// token is gob serializable
	Token      *oauth2.Token
	OAuthState string
}

func setCookie(w http.ResponseWriter, value *session) error {
	encoded, err := cookies.Encode(cookieName, value)
	if err != nil {
		return err
	}

	cookie := &http.Cookie{
		Name:     cookieName,
		Value:    encoded,
		Path:     "/",
		HttpOnly: true,
	}
	http.SetCookie(w, cookie)
	log.Println("set cookie?", encoded)
	return nil
}

// Returns the session for this user, or creates a new one and saves it.
func getOrCreateCookie(w http.ResponseWriter, r *http.Request) (*session, error) {
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		if err == http.ErrNoCookie {
			session := &session{nil, makeState()}
			err := setCookie(w, session)
			if err != nil {
				return nil, err
			}
			return session, nil
		}
		return nil, err
	}

	session := &session{}
	err = cookies.Decode(cookieName, cookie.Value, &session)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func makeState() string {
	randomState := make([]byte, 8)
	_, err := rand.Read(randomState)
	if err != nil {
		panic(err)
	}
	stateString := hex.EncodeToString(randomState)
	fmt.Println("state string", stateString)
	return stateString
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

func handleOauth2Start(w http.ResponseWriter, r *http.Request) {
	// Require POST (with CSRF protection) so users must take an explicit action on our site
	// See http://www.oauthsecurity.com/
	if r.Method != "POST" {
		http.Error(w, "Must use POST to start authentication", http.StatusMethodNotAllowed)
		return
	}
	log.Print("oauth2 start")

	// cookie the browser then redirect
	session, err := getOrCreateCookie(w, r)
	if err != nil {
		panic(err)
	}

	http.Redirect(w, r, bigQueryOAuthConfig.AuthCodeURL(session.OAuthState), http.StatusFound)
}

func handleOauth2Callback(w http.ResponseWriter, r *http.Request) {
	log.Print("oauth2 callback")

	errorString := r.FormValue("error")
	if errorString != "" {
		log.Println("oauth2 error", errorString)
		http.Error(w, "Authentication error: must grant access to BigQuery: "+errorString, http.StatusInternalServerError)
		return
	}

	session, err := getOrCreateCookie(w, r)
	if err != nil {
		log.Print("invalid cookie?", err.Error())
		http.Error(w, "Authentication error try again", http.StatusInternalServerError)
		return
	}

	state := r.FormValue("state")
	if state != session.OAuthState {
		log.Println("state mismatch", state, session.OAuthState)
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
	session.Token = token
	setCookie(w, session)
	log.Println("oauth successful")
	http.Redirect(w, r, "/view", http.StatusFound)
}

func handleView(w http.ResponseWriter, r *http.Request) {
	session, err := getOrCreateCookie(w, r)
	if session.Token == nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	ctx := context.Background()
	oauthClient := bigQueryOAuthConfig.Client(ctx, session.Token)
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
	fmt.Fprintf(w, "</body></html>")
}

func mustDecodeHex(hexString string) []byte {
	out, err := hex.DecodeString(hexString)
	if err != nil {
		panic(err)
	}
	return out
}

func main() {
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/oauth2start", handleOauth2Start)
	http.HandleFunc("/oauth2callback", handleOauth2Callback)
	http.HandleFunc("/view", handleView)

	const hostport = "localhost:8080"
	fmt.Printf("listening on http://%s/\n", hostport)

	// disable secure: we are serving over http for localhost
	csrfProtection := csrf.Protect(csrfKey, csrf.Secure(false))
	err := http.ListenAndServe(hostport, csrfProtection(http.DefaultServeMux))
	if err != nil {
		panic(err)
	}
}
