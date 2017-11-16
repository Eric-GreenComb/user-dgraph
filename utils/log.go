package utils

import (
	"log"
	"os"
	"path/filepath"
)

var networkErrLog string
var logdir string

func init() {
	logdir = CreateLogDirectory()
	networkErrLog = logdir + "/network_error.log"
}

func WriteNetworkError(stmt string) {
	stmt += "\n"

	f, err := os.OpenFile(networkErrLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Error while writing to network-log:", err)
		log.Println(stmt)
	}
	if _, err := f.Write([]byte(stmt)); err != nil {
		log.Println("Error while writing to network-log:", err)
		log.Println(stmt)
	}
	if err := f.Close(); err != nil {
		log.Println("Error while closing to network-logfile:", err)
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

func GetLogDir() string {
	return logdir
}

// Returns complete path of newly created directory
func CreateDirInsideLogDir(dirname string) (string, error) {
	dirpath := logdir + "/" + dirname
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		err := os.Mkdir(dirpath, 0777)
		return dirpath, err
	}
	return dirpath, nil
}
