package utils

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

var networkErrLog string

func init() {
	logdir := CreateLogDirectory()
	networkErrLog = logdir + "/network_error.log"
}

func WriteNetworkError(stmt string) {
	err := ioutil.WriteFile(networkErrLog, []byte(stmt), 0777)
	if err != nil {
		log.Println("Error while writing to network-log:",err)
		log.Println(stmt)
	}
}

func CreateLogDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	logdir := dir + "/logs"

	if _, err = os.Stat(logdir); os.IsNotExist(err) {
		os.Mkdir(logdir, 0777)
	}
	return logdir
}
