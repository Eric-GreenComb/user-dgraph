package dgraph

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
)

var (
	baseURL = flag.String("d", "http://user.dgraph.service.dci-wallet.consul:8080/query", "Dgraph server address")
)

func UpsertDgraph(query string) {

	log.Println("query =", query)

	client := &http.Client{}

	body := bytes.NewBufferString(query)

	req, err := http.NewRequest("POST", *baseURL, body)
	if err != nil {
		log.Fatal(err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer resp.Body.Close()

	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
		return
	}

	log.Println("Response=", string(responseData))

}
