package main

import (
	"encoding/hex"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"

	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/gorilla/securecookie"

	"./googlelogin"
)

// TODO: Load these in a configuration file!
// client id and secret for Google OAuth
const clientID = "329377969161-82blev2kcn2fqhppq6ns78jh67718jvb.apps.googleusercontent.com"
const clientSecret = "uu9G8NxNLeWbRTgvPgNbG_fl"
const redirectURL = "http://localhost:8080/oauth2callback"

// See http://www.gorillatoolkit.org/pkg/securecookie
const cookieHashKeyLength = 64
const cookieEncryptionKeyLength = 32

// Secure cookies
var cookieHashKey = mustDecodeHex("7b78e1662b9c4451a1b778814d0ae766cb3bcc521f87d38d126cd66cb37fcd7684c7eea08141e04b6ce5540c9bcd10ffe136a6711b24505b8813b6acefd3cfe2")
var cookieEncryptionKey = mustDecodeHex("3e385efa8cf1038b57f05091803282f9d0c0505c182831e301111bd33db8c9fe")

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

	fmt.Fprintf(w, `<html><body><form method="post" action="/oauth2start">
	<p>To use BigQuery backup  <input type="submit" value="sign in with google"></p>
	</form></body></html>`)
}

func (s *server) handleSuccess(w http.ResponseWriter, r *http.Request) {
	token, err := s.auth.GetToken(r)
	tokenvalid := false
	tokeninfo := ""
	var err2 error
	if token != nil {
		tokenvalid = token.Valid()
		tokeninfo, err2 = googlelogin.GetTokenInfo(token)
	}
	fmt.Fprintf(w, `<html><body><h1>Success</h1><p>token: %v err: %v</p>
		<p>token.Valid(): %v</p>
		<p>tokeninfo: %s %v</p>
		</body></html>`, token, err, tokenvalid, tokeninfo, err2)
}

type server struct {
	auth *googlelogin.Authenticator
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	err := s.auth.Start(w, r, "/success")
	if err != nil {
		panic(err)
	}
}

func (s *server) handleCallback(w http.ResponseWriter, r *http.Request) {
	log.Println("handleCallback")
	err := s.auth.HandleCallback(w, r)
	log.Println("callback handled:", err)
}

func projectsHandler(w http.ResponseWriter, r *http.Request, client *http.Client) {
	parts := strings.Split(r.URL.Path, "/")
	log.Printf("%s %s %d", r.URL.Path, parts, len(parts))
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	projectID := parts[2]
	if projectID == "" {
		log.Printf("%s = listProjects", r.URL.Path)
		listProjects(w, r, client)
	} else {
		log.Printf("%s = projectIndex(%s)", r.URL.Path, projectID)
		projectIndex(w, r, client, projectID)
	}
}

var listProjectsTemplate = template.Must(template.New("list").Parse(`<html><head>
<title>Big Query Projects</title>
</head>
<body>
<h1>Select a Project</h1>
<ul>
{{range .Projects}}
<li><a href="/projects/{{.Id}}">{{.FriendlyName}} ({{.Id}})</a></li>
{{end}}
</ul>
</body>
</html>`))

func listProjects(w http.ResponseWriter, r *http.Request, client *http.Client) {
	bq, err := bigquery.New(client)
	if err != nil {
		panic(err)
	}
	result, err := bq.Projects.List().Do()
	if err != nil {
		panic(err)
	}

	if result.NextPageToken != "" {
		panic("next page token")
	}
	err = listProjectsTemplate.Execute(w, result)
	if err != nil {
		panic(err)
	}
}

type projectIndexVars struct {
	FriendlyName string
	ProjectID    string
	Datasets     []*bigquery.DatasetListDatasets
}

var projectIndexTemplate = template.Must(template.New("projectIndex").Parse(`<html><head>
<title>Project {{.FriendlyName}} ({{.ProjectID}})</title>
</head>
<body>
<h1>{{.FriendlyName}}: Datasets</h1>
<ul>
{{range .Datasets}}
<li>{{.DatasetReference.DatasetId}} labels:{{.Labels}}</li>
{{end}}
</ul>
</body>
</html>`))

func projectIndex(w http.ResponseWriter, r *http.Request, client *http.Client, projectID string) {
	log.Printf("projectIndex %s", projectID)
	bq, err := bigquery.New(client)
	if err != nil {
		panic(err)
	}
	result, err := bq.Datasets.List(projectID).Do()
	if err != nil {
		panic(err)
	}

	if result.NextPageToken != "" {
		panic("next page token")
	}
	vars := &projectIndexVars{
		"friendly name",
		projectID,
		result.Datasets,
	}
	err = projectIndexTemplate.Execute(w, vars)
	if err != nil {
		panic(err)
	}
}

func main() {
	securecookies := securecookie.New(cookieHashKey, cookieEncryptionKey)
	auth := googlelogin.New(clientID, clientSecret, redirectURL,
		[]string{bigquery.BigqueryScope + ".readonly"}, securecookies, "/noauth")
	server := &server{auth}

	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/success", server.handleSuccess)
	http.HandleFunc("/start", server.handleStart)
	http.HandleFunc("/oauth2callback", server.handleCallback)

	http.Handle("/projects/", auth.Handler(projectsHandler))

	const hostport = "localhost:8080"
	fmt.Printf("listening on http://%s/\n", hostport)

	err := http.ListenAndServe(hostport, nil)
	if err != nil {
		panic(err)
	}
}
