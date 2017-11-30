package analyse

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
	"log"
	"os"
	"testing"
	"time"
)

var QueryThershold = 1000

func TestMatchedCases(t *testing.T) {
	dir := "/Users/ajayk/Documents/dgraph/dropshippers"
	matchedFPath := dir + "/compareResultsNotMatch_2.txt"

	matchedF, err := os.Open(matchedFPath)
	if err != nil {
		log.Fatal(err)
	}
	defer matchedF.Close()

	db, err := sql.Open("postgres", connDataWH)
	scanner := bufio.NewScanner(matchedF)

	cashbackF, err := os.Create(dir + "/cashback_not_matched_all.txt")
	defer cashbackF.Close()

	for scanner.Scan() {
		srn := scanner.Text()
		q := fmt.Sprintf(`SELECT payment_id FROM ws_order WHERE shipping_ref_num = '%s' ORDER BY create_time`, srn)
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

		q = fmt.Sprintf(`select payment_id, benefit_status, cashback_tokocash_given_amount from promo_code_usage_orders where payment_id in (%s)`, paymentIdStr)

		anotherRows, err := db.Query(q)
		if err != nil {
			log.Fatal(err)
		}
		defer anotherRows.Close()

		paymentIdCashbackMap := make(map[string]string)
		counter := 0

		for anotherRows.Next() {
			counter++
			var paymentId, benefitStatus, cashback string
			if err = anotherRows.Scan(&paymentId, &benefitStatus, &cashback); err != nil {
				log.Fatal(err)
			}
			paymentIdCashbackMap[paymentId] = fmt.Sprintf("%s,%s,%s,", paymentId, benefitStatus, cashback)
		}

		msg := srn + ","

		for _, pId := range paymentIds {
			msg += paymentIdCashbackMap[pId]
		}

		cashbackF.WriteString(msg + "\n")

	}
}

func TestMatchedCasesV2(t *testing.T) {
	defer utils.PrintTimeElapsed(time.Now(), "Total time spent:")
	dir := "/Users/ajayk/Documents/dgraph/dropshippers"
	matchedFPath := dir + "/compareResultsNotMatch_2.txt"

	matchedF, err := os.Open(matchedFPath)
	if err != nil {
		log.Fatal(err)
	}
	defer matchedF.Close()

	db, err := sql.Open("postgres", connDataWH)
	scanner := bufio.NewScanner(matchedF)

	cashbackF, err := os.Create(dir + "/cashback_not_matched_all_n.txt")
	defer cashbackF.Close()

	counter := 0
	srnsString := ""

	for scanner.Scan() {
		srnsString += fmt.Sprintf("'%s',", scanner.Text())
		counter++
		if counter == QueryThershold {
			writeSrnsCashbackData(srnsString, db, cashbackF)
			//Resetting
			counter = 0
			srnsString = ""
		}
	}
	if counter != 0 {
		writeSrnsCashbackData(srnsString, db, cashbackF)
	}
}

func writeSrnsCashbackData(srnsString string, db *sql.DB, cashbackF *os.File) {
	srnsString = srnsString[:len(srnsString)-1]
	q := fmt.Sprintf(`SELECT payment_id, item_price, shipping_ref_num FROM ws_order WHERE shipping_ref_num IN (%s) ORDER BY create_time`, srnsString)
	rows, err := db.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	SRNsPaymentMap := make(map[string][]string)
	paymentIdPriceMap := make(map[string]string)
	for rows.Next() {
		var paymentId string
		var itemPrice string
		var srn string
		if err = rows.Scan(&paymentId, &itemPrice, &srn); err != nil {
			log.Fatal(err)
		}

		paymentIdPriceMap[paymentId] = itemPrice
		SRNsPaymentMap[srn] = append(SRNsPaymentMap[srn], paymentId)

	}
	for srn, pIds := range SRNsPaymentMap {
		pidsStr := ""

		for _, pId := range pIds {
			pidsStr += fmt.Sprintf("'%s',", pId)
		}
		pidsStr = pidsStr[:len(pidsStr)-1]
		q = fmt.Sprintf(`select payment_id, cashback_tokocash_given_amount from promo_code_usage_orders where payment_id in (%s)`, pidsStr)

		anotherRows, err := db.Query(q)
		if err != nil {
			log.Fatal(err)
		}
		defer anotherRows.Close()

		pIdMsgMap := make(map[string]string)

		for anotherRows.Next() {
			var paymentId, cashback string
			if err = anotherRows.Scan(&paymentId, &cashback); err != nil {
				log.Fatal(err)
			}
			pIdMsgMap[paymentId] = fmt.Sprintf("%s,%s,%s,", paymentId, paymentIdPriceMap[paymentId], cashback)
		}

		msg := srn + ","
		for _, pId := range pIds {
			msg += pIdMsgMap[pId]
		}
		cashbackF.WriteString(msg + "\n")
	}
}

func TestMatchedCasesV3(t *testing.T) {
	defer utils.PrintTimeElapsed(time.Now(), "Total time spent:")
	dir := "/Users/ajayk/Documents/dgraph/dropshippers/analysis"
	matchedFPath := dir + "/soron_all_srns.txt"

	matchedF, err := os.Open(matchedFPath)
	if err != nil {
		log.Fatal(err)
	}
	defer matchedF.Close()

	db, err := sql.Open("postgres", connDataWH)
	scanner := bufio.NewScanner(matchedF)

	cashbackF, err := os.Create(dir + "/cashback_all_soron.txt")
	defer cashbackF.Close()

	counter := 0
	srnsString := ""

	for scanner.Scan() {
		srnsString += fmt.Sprintf("'%s',", scanner.Text())
		counter++
		if counter == QueryThershold {
			writeSrnsCashbackDataV2(srnsString, db, cashbackF)
			//Resetting
			counter = 0
			srnsString = ""
		}
	}
	if counter != 0 {
		writeSrnsCashbackDataV2(srnsString, db, cashbackF)
	}
}

func writeSrnsCashbackDataV2(srnsString string, db *sql.DB, cashbackF *os.File) {
	srnsString = srnsString[:len(srnsString)-1]
	q := fmt.Sprintf(`SELECT payment_id, item_price, shipping_ref_num, create_time, phone FROM ws_order WHERE shipping_ref_num IN (%s) ORDER BY create_time`, srnsString)
	rows, err := db.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	SRNsPaymentMap := make(map[string][]string)
	paymentIdPriceMap := make(map[string]string)
	paymentIdCreateTimeMap := make(map[string]string)
	paymentIdPhoneMap := make(map[string]string)
	paymentIdStr := ""
	for rows.Next() {
		var paymentId string
		var itemPrice string
		var srn string
		var createTime string
		var phone string
		if err = rows.Scan(&paymentId, &itemPrice, &srn, &createTime, &phone); err != nil {
			log.Fatal(err)
		}

		paymentIdPriceMap[paymentId] = itemPrice
		SRNsPaymentMap[srn] = append(SRNsPaymentMap[srn], paymentId)
		paymentIdCreateTimeMap[paymentId] = createTime
		paymentIdStr += fmt.Sprintf("'%s',", paymentId)
		paymentIdPhoneMap[paymentId] = phone
	}
	paymentIdStr = paymentIdStr[:len(paymentIdStr)-1]

	q = fmt.Sprintf(`select payment_id, cashback_tokocash_given_amount, promo_id from promo_code_usage_orders where payment_id in (%s)`, paymentIdStr)
	anotherRows, err := db.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	defer anotherRows.Close()

	pIdCashbackMap := make(map[string]string)
	pIdPromoIdMap := make(map[string]string)

	for anotherRows.Next() {
		var paymentId, cashback, promoId string
		if err = anotherRows.Scan(&paymentId, &cashback, &promoId); err != nil {
			log.Fatal(err)
		}
		pIdCashbackMap[paymentId] = cashback
		pIdPromoIdMap[paymentId] = promoId
	}

	for srn, pIds := range SRNsPaymentMap {
		var phones []string
		msg := ""
		for _, pId := range pIds {
			phone := utils.NormalizePhone(paymentIdPhoneMap[pId])
			msg += fmt.Sprintf("%s,%s,%s,%s,%s,%s,", pId, paymentIdPriceMap[pId], pIdCashbackMap[pId], paymentIdCreateTimeMap[pId], phone, pIdPromoIdMap[pId])
			phones = append(phones, phone)
		}

		if utils.AllEqual(phones) {
			msg = fmt.Sprintf(",T,") + msg
		} else {
			msg = fmt.Sprintf(",F,") + msg
		}
		msg = srn + msg
		cashbackF.WriteString(msg + "\n")
	}
}
