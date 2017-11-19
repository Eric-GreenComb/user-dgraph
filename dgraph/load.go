package dgraph

import (
	"bytes"
	"flag"
	"github.com/tokopedia/user-dgraph/utils"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	baseURL = flag.String("d", "http://user.dgraph.service.dci-wallet.consul:8080/query", "Dgraph server address")
	//http://user.dgraph.service.dci-wallet.consul:8080/query
)

func UpsertDgraph(query string) string {

	//log.Println("query =", query)
	message := "OK"

	client := &http.Client{}

	body := bytes.NewBufferString(query)

	req, err := http.NewRequest("POST", *baseURL, body)
	if err != nil {
		message = time.Now().String() + err.Error()
		utils.WriteNetworkError(message)
		//log.Fatal(err)
		return message
	}

	resp, err := client.Do(req)
	if err != nil {
		message = time.Now().String() + err.Error()
		utils.WriteNetworkError(message)
		//log.Fatal(err)
		return message
	}
	defer resp.Body.Close()

	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		message = time.Now().String() + err.Error()
		utils.WriteNetworkError(message)
		//log.Fatal(err)
		return message
	}

	responseDataStr := string(responseData)
	if strings.Contains(strings.ToUpper(responseDataStr), "ERROR") {
		message = time.Now().String() + responseDataStr
		utils.WriteNetworkError(message)
		return message
	} else {
		message = responseDataStr
		log.Println("Response=", message)
		return message
	}

}
