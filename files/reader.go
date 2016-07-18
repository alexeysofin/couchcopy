package files

import (
	"bufio"
	"io"
	"net/http"
	"os"
)

func NewReader(url string) (io.Reader, error) {
	// returns a io.Reader interface depending on path is a url or not
	if IsPathUrl(url) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		return resp.Body, nil
	}
	fp, err := os.Open(url)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(fp), nil
}
