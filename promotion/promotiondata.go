package promotion

import (
	"database/sql"
	"fmt"
	"github.com/tokopedia/user-dgraph/dgraph"
	"github.com/tokopedia/user-dgraph/utils"
	"log"
	"os"
	"strconv"
	"time"
)

type PromoDataRequest struct {
	From      Date   `json:"from"`
	To        Date   `json:"to"`
	Promocode string `json:"promocode"`
}

func (date *Date) IsValid() bool {
	return date.Year < 2018 &&
		date.Year >= 2015 &&
		date.Month > 0 &&
		date.Month <= 12 &&
		date.Day > 0 &&
		date.Day <= 31
}

type PromoData struct {
	shippingRefNumber string
	buyerId           int64
	sellerId          int64
	shopId            int64
}

var (
	connDataWH = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		"10.164.4.46", "ab171011", "fU8nT4RBV", "tokopedia-trove")
)

type Date struct {
	Year  int        `json:"year"`
	Month time.Month `json:"month"`
	Day   int        `json:"day"`
}

/**
Promo Code -> PaymentID -> OrderID -> Buyer UserID; ShopID -> Seller UserID
*/

func GetPromotionData(promo string, dateFrom, dateTo Date) ([]PromoData, map[int64]int64) {
	defer utils.PrintTimeElapsed(time.Now(), "GetPromotionData Elapsed Time:")
	db, err := sql.Open("postgres", connDataWH)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()

	if err != nil {
		log.Fatal("Error: Could not establish a connection with the database")
		panic(err)
	}

	log.Println("CONN SUCCESS")

	from := time.Date(dateFrom.Year, dateFrom.Month, dateFrom.Day, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	to := time.Date(dateTo.Year, dateTo.Month, dateTo.Day, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)

	count := getPaymentRefsCount(promo, from, to, db)

	limit := 10000
	offset := 0
	N := count / limit
	if count%limit != 0 {
		N += 1
	}

	type PromoDataWorker struct {
		promodata []PromoData
		shopIds   map[int64]bool
	}
	sem := make(chan PromoDataWorker, N)

	for offset < count {

		go func(offset int) {
			promodata, shopIds := getPromotionDataWithOffset(promo, from, to, limit, offset, db)
			log.Println("len:", len(promodata), len(shopIds))
			sem <- PromoDataWorker{promodata: promodata, shopIds: shopIds}
		}(offset)
		offset += limit
	}

	// wait for goroutines to finish
	var promoData []PromoData
	shopIdList := make(map[int64]bool)
	for i := 0; i < N; {
		i++
		promodataWorker := <-sem
		promoData = append(promoData, promodataWorker.promodata...)
		updateShopListMap(promodataWorker.shopIds, shopIdList)
	}

	shopSellerMap := getShopSellerMap(shopIdList, db)
	fmt.Println("TotalShopSellerLength:", len(shopSellerMap))

	return promoData, shopSellerMap

}

func updateShopListMap(source, destination map[int64]bool) {
	for k, _ := range source {
		destination[k] = true
	}
}

func getPaymentRefsCount(promo, from, to string, db *sql.DB) int {
	defer utils.PrintTimeElapsed(time.Now(), "getPaymentRefsCount elapsed time:")
	countQuery := fmt.Sprintf(`SELECT count(payment_id ) FROM ws_payment_promo_galadriel WHERE code = $1 AND create_time > $2 AND create_time < $3`)
	count := 0
	err := db.QueryRow(countQuery, promo, from, to).Scan(&count)
	if err != nil {
		log.Println("Error while getting the count:", err)
	}
	log.Println("Count:", count)
	return count
}

func getPromotionDataWithOffset(promo, dateFrom, dateTo string, limit, offset int, db *sql.DB) ([]PromoData, map[int64]bool) {
	defer utils.PrintTimeElapsed(time.Now(), "runPaymentRefQuery elapsed time:")
	query := fmt.Sprintf(`SELECT payment_id FROM ws_payment_promo_galadriel WHERE code = $1 AND create_time > $2 AND create_time < $3 LIMIT $4 OFFSET $5`)

	var promoData []PromoData
	rows, err := db.Query(query, promo, dateFrom, dateTo, limit, offset)
	if err != nil {
		log.Println(err)
		return promoData, nil
	}
	defer rows.Close()

	var paymentIds []int64
	for rows.Next() {
		var paymentId int64
		if err = rows.Scan(&paymentId); err != nil {
			log.Println(err)
			continue
		}
		paymentIds = append(paymentIds, paymentId)
	}
	log.Println("Length of PaymentIds received:", len(paymentIds))
	promoData, shopIds := getOrderDataByPaymentIDs(paymentIds, db)
	return promoData, shopIds
}

func getShopSellerMap(shopIdList map[int64]bool, db *sql.DB) map[int64]int64 {
	shopSellerMap := make(map[int64]int64)
	sellerList := HashSetToCSV(shopIdList)
	query := fmt.Sprintf(`SELECT shop_id,user_id FROM ws_shop WHERE shop_id in (%s)`, sellerList)

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return shopSellerMap
	}
	defer rows.Close()

	for rows.Next() {
		var shopId, sellerId int64
		if err = rows.Scan(&shopId, &sellerId); err != nil {
			log.Println(err)
			continue
		}
		shopSellerMap[shopId] = sellerId
	}
	return shopSellerMap
}

func getOrderDataByPaymentIDs(paymentIds []int64, db *sql.DB) ([]PromoData, map[int64]bool) {
	defer utils.PrintTimeElapsed(time.Now(), "getOrderDataByPaymentIDs elapsedTime:")
	paymentIdsStr := SliceToCSV(paymentIds)
	query := fmt.Sprintf(`SELECT shop_id, customer_id, shipping_ref_num FROM ws_order WHERE payment_id IN (%s)`, paymentIdsStr)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, nil
	}
	defer rows.Close()

	var promoDataList []PromoData
	shopIds := make(map[int64]bool)

	for rows.Next() {
		var shopId, customerId int64
		var shippingRefNum string

		if err = rows.Scan(&shopId, &customerId, &shippingRefNum); err != nil {
			continue
		}
		promoDataList = append(promoDataList, PromoData{shopId: shopId, buyerId: customerId, shippingRefNumber: shippingRefNum})
		shopIds[shopId] = true

	}
	//log.Println("shop_id, customer_id, shipping_ref_num:", shopId, customerId, shippingRefNum)
	return promoDataList, shopIds
}

func SliceToCSV(list []int64) string {
	output := ""
	for _, v := range list {
		output += strconv.FormatInt(v, 10) + ","
	}
	output = output[:len(output)-1]
	return output
}
func HashSetToCSV(hashset map[int64]bool) string {
	output := ""
	for k, _ := range hashset {
		output += strconv.FormatInt(k, 10) + ","
	}
	output = output[:len(output)-1]
	return output
}

func WritetoDgraph(promoDataList []PromoData, shopSellerMap map[int64]int64, logfile *os.File) {
	defer logfile.WriteString(fmt.Sprintf("Total time spent in dgraph writing:%v", utils.GetTimeElapsed(time.Now())))

	query :=
		`{
			buyer  as var(func: eq(user_id, "%v"))      @upsert
			seller as var(func: eq(user_id, "%v"))      @upsert
			s_r    as var(func: eq(ship_ref_num, "%v")) @upsert
		}

		mutation {
		  set {
			uid(s_r) <name> "Shipping Ref Number" .
			uid(buyer) <name> "USER" .
			uid(seller) <name> "USER" .
			uid(s_r) <buyer> uid(buyer) .
			uid(s_r) <seller> uid(seller) .
		  }
		}`

	for _, promoData := range promoDataList {
		buyer := promoData.buyerId
		shipRefNum := promoData.shippingRefNumber
		seller := shopSellerMap[promoData.shopId]
		if buyer == 0 || seller == 0 || shipRefNum == "" {
			log.Println("Invalid promodata:buyer,seller,shipRefNum:", buyer, seller, shipRefNum)
			continue
		}

		logfile.WriteString(promoData.shippingRefNumber + "\n")

		dgraph.UpsertDgraph(fmt.Sprintf(query, buyer, seller, shipRefNum))

	}
}
