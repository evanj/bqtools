# BigQuery Tools

A tool to figure out what costs money in a Google BigQuery project. Try it at https://bigquery-tools.appspot-preview.com/ Send feedback to ej@evanjones.ca.


## Deploying

1. Get a BigQuery OAuth Client ID: https://console.cloud.google.com/apis/credentials . Add https://yourdomain.appspot-preview.com/oauth2callback as the OAuth2 callback URL. If you want to test this locally, you should also add localhost:8080.

2. Generate a secure cookie hash key: `openssl rand -hex 64` and encryption key: `openssl rand -hex 32`

3. Save these values as constants in `credentials.go`:
  ```
  package main

  const googleOAuthClientID = "..."
  const googleOAuthClientSecret = "..."

  var cookieHashKey = mustDecodeHex("...")
  var cookieEncryptionKey = mustDecodeHex("...")
  ```

4. Create a Cloud SQL instance. Edit bqcost.go and bqcost.yaml to reference the correct name.

5. Edit bqcost.go to reference the correct host name that will serve your app.

6. Deploy: `aedeploy gcloud app deploy --project=(PROJECT) bqcost.yaml`


## Running locally

You can run a local copy against cloud SQL with `go run bqcost.go credentials.go --cloudSQLProxy=true`

You can also run a local copy using SQLite, but I need to figure out a way to make this work without breaking deploys to App Engine Flexible.


## Known Issues

* Downloading the state of all the tables is incredibly slow. It needs to be parallelized.
* The "get table data from BigQuery" job is just a goroutine. If the instance restarts, the job is stuck forever and you will never be able to access the project. This should be split into smaller chunks that get retried.
