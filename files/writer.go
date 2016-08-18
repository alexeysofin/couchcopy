package files

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/alexeysofin/couchcopy/common"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
)

type BulkDocs struct {
	Docs []interface{} `json:"docs"`
}

func WriteToRemote(dbUrl string, docs []interface{}) {
	// Writes array of docs to http url

	// append _bulk_docs to url, so we could upload in large chunks
	finalUrl, err := url.Parse(dbUrl)
	common.CheckError(err)
	finalUrl.Path = path.Join(finalUrl.Path, "_bulk_docs")
	dbUrl = finalUrl.String()

	// make a bulk structure
	bulkd := BulkDocs{Docs: docs}
	body := new(bytes.Buffer)

	// encode it to json
	json.NewEncoder(body).Encode(bulkd)

	// make an http request
	client := http.Client{}
	request, err := http.NewRequest("POST", dbUrl, body)
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response, err := client.Do(request)
	common.CheckError(err)

	responseBody, _ := ioutil.ReadAll(response.Body)

	if response.StatusCode != 201 {
		panic(fmt.Sprintf("%d, %s, %s", response.StatusCode, dbUrl, string(responseBody)))
	}

	response.Body.Close()
}

func WriteStreamToFile(filePath string, reader io.Reader) (int64, error) {
	// writes reader contents to file at given path

	fp, err := os.Create(filePath)
	common.CheckError(err)
	return io.Copy(fp, reader)
}

func WriteLineToFile(writer *bufio.Writer, body []byte) error {
	// writes body + new line to a file writer

	if _, err := writer.WriteString(fmt.Sprintf("%s\n", string(body))); err != nil {
		return err
	}

	return nil
}
