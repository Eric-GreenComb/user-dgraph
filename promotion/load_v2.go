package promotion

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	protos "github.com/dgraph-io/dgraph/protos/api"
	"github.com/tokopedia/user-dgraph/dgraph"
	"github.com/tokopedia/user-dgraph/utils"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

type BuyerRoot struct {
	Buyer  []UserDgraph     `json:"buyer"`
	AWB    []ShippingRefNum `json:"awb"`
	Seller []UserDgraph     `json:"seller"`
}

type ShippingRefNum struct {
	UID            string `json:"uid,omitempty"`
	Name           string `json:"name,omitempty"`
	ShippingRefNum string `json:"ship_ref_num,omitempty"`
}

func LoadDgraph(dir string) error {
	dirpath := utils.GetLogDir() + "/" + dir
	shopSellerMapFile := dirpath + "/shop_seller_map"

	if _, err := os.Stat(shopSellerMapFile); os.IsNotExist(err) {
		msg := fmt.Sprintf("ShopSellerMap file doesn't exist cant process,path:%s", shopSellerMapFile)
		log.Println(msg)
		return errors.New(msg)
	}

	shopSellerMap, err := getSellerMap(shopSellerMapFile)
	if err != nil {
		return err
	}

	logfile, err := os.OpenFile(dirpath+"/log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Println("Error while creating/writing to log:", err)

	}
	defer logfile.Close()

	scanner := bufio.NewScanner(logfile)
	processedFiles := make(map[string]bool)

	for scanner.Scan() {
		processedFiles[scanner.Text()] = true
	}

	files, err := ioutil.ReadDir(dirpath)
	if err != nil {
		log.Println("Couldn't open the directory:", err)
		return err
	}

	c := dgraph.GetClient()
	starttime := time.Now()
	log.Println("Relationship creation started at:", starttime)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "data") && !processedFiles[f.Name()] {
			t1 := time.Now()
			log.Println("Processing:", dirpath+"/"+f.Name())

			file, err := os.Open(dirpath + "/" + f.Name())
			if err != nil {
				return err
			}
			defer file.Close()
			scandata := bufio.NewScanner(file)

			for scandata.Scan() {
				str := strings.Split(scandata.Text(), ",")
				err = search(c, str[0], str[1], shopSellerMap[str[2]])
				if err != nil {
					return err
				}
			}
			logfile.WriteString(fmt.Sprintf("%s\n", f.Name()))
			log.Println(fmt.Sprintf("Completed processing::%v\n\n", time.Since(t1)))
		}
	}
	log.Println("All relationship created, time spent:", utils.GetTimeElapsed(starttime))

	return nil

}

func getSellerMap(filepath string) (map[string]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanOrder := bufio.NewScanner(file)

	m := make(map[string]string)

	for scanOrder.Scan() {
		str := strings.Split(scanOrder.Text(), ",")
		m[str[0]] = str[1]
	}

	return m, nil
}

func search(ct *client.Dgraph, awb, cust, seller string) error {

	q := `
	{
		awb(func: eq(ship_ref_num, %q)) {
			uid

		}
		buyer(func: eq(user_id, %q)) {
			uid
		}

		seller(func: eq(user_id, %q)) {
			uid
		}
	}
	`
	x := fmt.Sprintf(q, awb, cust, seller)

	//ct := newClient()
	txn := ct.NewTxn()

	resp, err := txn.Query(context.Background(), x)

	if err != nil {
		return err
	}

	var r BuyerRoot
	//fmt.Println("r = ", r)
	//fmt.Println("r = ", resp)
	err = json.Unmarshal(resp.Json, &r)
	if err != nil {
		return err
	}

	return upsertAWB(r, txn, awb, cust, seller)
}

func upsertAWB(r BuyerRoot, txn *client.Txn, awb, cust, seller string) error {
	q := `
	%v <user_id> %q .
	%v <user_id> %q .
	%v <name> "USER" .
	%v <name> "USER" .
	%v <ship_ref_num> %q .
	%v <name> "ShippingRefNumber" .
	%v <buyer> %v .
	%v <seller> %v .
	`
	var b, s, ref string

	if len(r.AWB) == 0 {
		ref = "_:ref"
	} else {
		ref = "<" + r.AWB[0].UID + ">"
	}

	if len(r.Buyer) == 0 {
		b = "_:b"
	} else {
		b = "<" + r.Buyer[0].Uid + ">"
	}

	if len(r.Seller) == 0 {
		s = "_:s"
	} else {
		s = "<" + r.Seller[0].Uid + ">"
	}

	x := fmt.Sprintf(q,
		b, cust,
		s, seller,
		b, s,
		ref, awb,
		ref,
		ref, b,
		ref, s)

	log.Println(x)

	mu := &protos.Mutation{SetNquads: []byte(x)}
	_, err := txn.Mutate(context.Background(), mu)

	if err != nil {
		return err
	}

	err = txn.Commit(context.Background())

	return err
}

func runSchemaBuyer(dg *client.Dgraph) {

	op := &protos.Operation{}
	op.Schema = `
		ship_ref_num: string @index(exact) .
		buyer: uid @reverse @count .
		seller: uid @reverse @count .
		user_id: int @index(int) .
		`

	ctx := context.Background()
	err := dg.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}
}
