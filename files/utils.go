package files

import (
	"strings"
)

func IsPathUrl(path string) bool {
	// checks whether given path is a url or a file path
	return strings.HasPrefix(path, "http")
}
