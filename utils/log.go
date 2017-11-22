package utils

import (
	"fmt"
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

func WriteInt64HashSetToFile(hashset map[int64]bool, dataDirPath, filename string) error {
	filep := fmt.Sprintf("%s/%s", dataDirPath, filename)
	f, err := os.Create(filep)
	if err != nil {
		log.Println("Error while creating file:", filep, err)
		return err
	}
	defer f.Close()

	for k, _ := range hashset {
		stmt := fmt.Sprintf("%d\n", k)
		if _, err := f.WriteString(stmt); err != nil {
			log.Println("Error while writing to file:", filep, err)
			return err
		}
	}
	return nil
}

func WriteStringHashSetToFile(hashset map[string]bool, dataDirPath, filename string) error {
	filep := fmt.Sprintf("%s/%s", dataDirPath, filename)
	f, err := os.Create(filep)
	if err != nil {
		log.Println("Error while creating file:", filep, err)
		return err
	}
	defer f.Close()

	for k, _ := range hashset {
		stmt := fmt.Sprintf("%s\n", k)
		if _, err := f.WriteString(stmt); err != nil {
			log.Println("Error while writing to file:", filep, err)
			return err
		}
	}
	return nil
}
