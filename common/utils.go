package common

import (
	"log"
)

func CheckError(e error) {
	// Checks where error has occurred
	if e != nil {
		log.Fatal(e.Error())
	}
}
