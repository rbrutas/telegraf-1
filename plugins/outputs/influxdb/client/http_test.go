package client

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	// "github.com/influxdata/telegraf/testutil"

	"github.com/stretchr/testify/assert"
)

func TestHTTPClient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/write":
			// test form values:
			if r.FormValue("db") != "test" {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"wrong db name"}`)
			}
			if r.FormValue("rp") != "policy" {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"wrong rp name"}`)
			}
			if r.FormValue("precision") != "ns" {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"wrong precision"}`)
			}
			if r.FormValue("consistency") != "all" {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"wrong consistency"}`)
			}
			// test that user agent is set properly
			if r.UserAgent() != "test-agent" {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"wrong agent name"}`)
			}
			// test basic auth params
			user, pass, ok := r.BasicAuth()
			if !ok {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"basic auth not set"}`)
			}
			if user != "test-user" || pass != "test-password" {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"basic auth incorrect"}`)
			}

			// Validate Content-Length Header
			if r.ContentLength != 13 {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				msg := fmt.Sprintf(`{"results":[{}],"error":"Content-Length: expected [13], got [%d]"}`, r.ContentLength)
				fmt.Fprintln(w, msg)
			}

			// Validate the request body:
			buf := make([]byte, 100)
			n, _ := r.Body.Read(buf)
			expected := "cpu value=99"
			got := string(buf[0 : n-1])
			if expected != got {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				msg := fmt.Sprintf(`{"results":[{}],"error":"expected [%s], got [%s]"}`, expected, got)
				fmt.Fprintln(w, msg)
			}

			w.WriteHeader(http.StatusNoContent)
			w.Header().Set("Content-Type", "application/json")
		case "/query":
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"results":[{}]}`)
		}
	}))
	defer ts.Close()

	config := HTTPConfig{
		URL:       ts.URL,
		UserAgent: "test-agent",
		Username:  "test-user",
		Password:  "test-password",
	}
	wp := WriteParams{
		Database:        "test",
		RetentionPolicy: "policy",
		Precision:       "ns",
		Consistency:     "all",
	}
	client, err := NewHTTP(config, wp)
	defer client.Close()
	assert.NoError(t, err)
	n, err := client.Write([]byte("cpu value=99\n"))
	assert.Equal(t, 13, n)
	assert.NoError(t, err)

	_, err = client.WriteStream(bytes.NewReader([]byte("cpu value=99\n")), 13)
	assert.NoError(t, err)
}

func TestHTTPClient_WriteParamsOverride(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/write":
			// test that database is set properly
			if r.FormValue("db") != "override" {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintln(w, `{"results":[{}],"error":"wrong db name"}`)
			}

			// Validate the request body:
			buf := make([]byte, 100)
			n, _ := r.Body.Read(buf)
			expected := "cpu value=99"
			got := string(buf[0 : n-1])
			if expected != got {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				msg := fmt.Sprintf(`{"results":[{}],"error":"expected [%s], got [%s]"}`, expected, got)
				fmt.Fprintln(w, msg)
			}

			w.WriteHeader(http.StatusNoContent)
			w.Header().Set("Content-Type", "application/json")
		case "/query":
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"results":[{}]}`)
		}
	}))
	defer ts.Close()

	config := HTTPConfig{
		URL: ts.URL,
	}
	defaultWP := WriteParams{
		Database: "test",
	}
	client, err := NewHTTP(config, defaultWP)
	defer client.Close()
	assert.NoError(t, err)

	// test that WriteWithParams overrides the default write params
	wp := WriteParams{
		Database: "override",
	}
	n, err := client.WriteWithParams([]byte("cpu value=99\n"), wp)
	assert.Equal(t, 13, n)
	assert.NoError(t, err)

	_, err = client.WriteStreamWithParams(bytes.NewReader([]byte("cpu value=99\n")), 13, wp)
	assert.NoError(t, err)
}

func TestNewHTTPErrors(t *testing.T) {
	// No URL:
	config := HTTPConfig{}
	defaultWP := WriteParams{
		Database: "test",
	}
	client, err := NewHTTP(config, defaultWP)
	assert.Error(t, err)
	assert.Nil(t, client)

	// No Database:
	config = HTTPConfig{
		URL: "http://localhost:8086",
	}
	defaultWP = WriteParams{}
	client, err = NewHTTP(config, defaultWP)
	assert.Nil(t, client)
	assert.Error(t, err)

	// Invalid URL:
	config = HTTPConfig{
		URL: "http://192.168.0.%31:8080/",
	}
	defaultWP = WriteParams{
		Database: "test",
	}
	client, err = NewHTTP(config, defaultWP)
	assert.Nil(t, client)
	assert.Error(t, err)

	// Invalid URL scheme:
	config = HTTPConfig{
		URL: "mailto://localhost:8086",
	}
	defaultWP = WriteParams{
		Database: "test",
	}
	client, err = NewHTTP(config, defaultWP)
	assert.Nil(t, client)
	assert.Error(t, err)
}

func TestHTTPClient_Query(t *testing.T) {
	command := "CREATE DATABASE test"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/write":
			w.WriteHeader(http.StatusNoContent)
		case "/query":
			// validate the create database command is correct
			got := r.FormValue("q")
			if got != command {
				w.WriteHeader(http.StatusTeapot)
				w.Header().Set("Content-Type", "application/json")
				msg := fmt.Sprintf(`{"results":[{}],"error":"got %s, expected %s"}`, got, command)
				println(msg)
				fmt.Fprintln(w, msg)
			}

			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"results":[{}]}`)
		}
	}))
	defer ts.Close()

	config := HTTPConfig{
		URL: ts.URL,
	}
	defaultWP := WriteParams{
		Database: "test",
	}
	client, err := NewHTTP(config, defaultWP)
	defer client.Close()
	assert.NoError(t, err)
	err = client.Query(command)
	assert.NoError(t, err)
}
