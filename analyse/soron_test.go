package analyse

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"testing"
)

var (
	connDataWH = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		"10.164.4.46", "ab171011", "fU8nT4RBV", "tokopedia-trove")
)

func TestAnalyseSoron(t *testing.T) {
	dir := "/Users/ajayk/Documents/dgraph/dropshippers"
	//dgraphSRNsPath := dir + "/compareResultsMatch.txt"
	//excelSRNsPath := dir + "/excel.txt"
	extraExcelPath := dir + "/extraExcel.txt"

	extraExcelF, err := os.Open(extraExcelPath)
	if err != nil {
		log.Fatal(err)
	}
	defer extraExcelF.Close()

	db, err := sql.Open("postgres", connDataWH)

	scanner := bufio.NewScanner(extraExcelF)

	var logLessFitIntoCond []string
	var missedOnes []string
	//extraExcelSRNs := ""
	for scanner.Scan() {
		srn := scanner.Text()

		q := fmt.Sprintf(`select payment_id from ws_order where shipping_ref_num = '%s'`, srn)

		rows, err := db.Query(q)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		var paymentIds []string
		var paymentIdStr string
		for rows.Next() {
			var paymentId string
			if err = rows.Scan(&paymentId); err != nil {
				log.Fatal(err)
			}
			paymentIds = append(paymentIds, paymentId)
			paymentIdStr += fmt.Sprintf("'%s',", paymentId)
		}

		if len(paymentIds) < 2 {
			log.Println(fmt.Sprintf("%s:only one payment id:%s", srn, paymentIds[0]))
			continue
		}

		paymentIdStr = paymentIdStr[:len(paymentIdStr)-1]

		q = fmt.Sprintf(`select payment_id, create_time from ws_payment_promo_galadriel where payment_id in(%s) and code = 'CASHBACKPASTI' and create_time > '2017-11-09' and create_time < '2017-11-12'`, paymentIdStr)

		anotherRows, err := db.Query(q)
		if err != nil {
			log.Fatal(err)
		}
		defer anotherRows.Close()

		paymentIdTimeMap := make(map[string]string)
		counter := 0
		for anotherRows.Next() {
			counter++
			var paymentId, createTime string
			if err = anotherRows.Scan(&paymentId, &createTime); err != nil {
				log.Fatal(err)
			}
			paymentIdTimeMap[paymentId] = createTime
		}

		if len(paymentIds) != counter {
			logLessFitIntoCond = append(logLessFitIntoCond, fmt.Sprintf("%s:pIds-%v:got-%v", srn, paymentIds, paymentIdTimeMap))
			continue
		}
		missedOnes = append(missedOnes, fmt.Sprintf("%s:%v", srn, paymentIdTimeMap))
		//extraExcelSRNs += fmt.Sprintf("'%s',", scanner.Text())
	}

	log.Println("Following doesn't fit into conditions:")
	for _, txt := range logLessFitIntoCond {
		log.Println(txt)
	}

	log.Println("Mysterious ones:")
	for _, txt := range missedOnes {
		log.Println(txt)
	}
	//extraExcelSRNs = extraExcelSRNs[:len(extraExcelSRNs)-1]

	/*query := fmt.Sprintf(`select distinct(shipping_ref_num), create_time from ws_order where
	shipping_ref_num in (%s) and create_time > '2017-11-09' and create_time < '2017-11-12'`, extraExcelSRNs)

		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		counter := 0
		for rows.Next() {
			counter++
			var shipRef, createTime string
			if err = rows.Scan(&shipRef, &createTime); err != nil {
				log.Fatal(err)
			}
			fmt.Println(fmt.Sprintf("%s@%s", shipRef, createTime))
		}
		fmt.Println(counter)*/
}

func TestAnotherlife(t *testing.T) {
	var list []string
	list = append(list, "kk")
	list = append(list, "ajay")
	log.Println(fmt.Sprintf("%v", list))

	amap := make(map[string]string)
	amap["ajay"] = "kumar"
	amap["shamita"] = "b"
	amap["amisha"] = "chaj"
	log.Println(fmt.Sprintf("%s:%v:%v", "love", amap, list))
}
