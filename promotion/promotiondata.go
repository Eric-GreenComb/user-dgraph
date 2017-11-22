package promotion

import (
	"database/sql"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
	"log"
	"os"
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

type Date struct {
	Year  int        `json:"year"`
	Month time.Month `json:"month"`
	Day   int        `json:"day"`
}

func (this *Date) Equal(date Date) bool {
	return this.Year == date.Year && this.Month == date.Month && this.Day == date.Day
}

func GetDate(time time.Time) Date {
	return Date{time.Year(), time.Month(), time.Day()}
}

func (date *Date) GetTime() time.Time {
	return time.Date(date.Year, date.Month, date.Day, 0, 0, 0, 0, time.UTC)
}
func (date *Date) ToString() string {
	return fmt.Sprintf("%d_%d_%d", date.Year, date.Month, date.Day)
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

/**
Promo Code -> PaymentID -> OrderID -> Buyer UserID; ShopID -> Seller UserID
*/

func GetPromotionData(dateFrom, dateTo Date, promo, dataDirPath string, metaFile *os.File) ([]PromoData, map[int64]int64, error) {
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

	from := dateFrom.GetTime().Format(time.RFC3339)
	to := dateTo.GetTime().Format(time.RFC3339)

	count := getPaymentRefsCount(promo, from, to, db)

	limit := 10000
	offset := 0
	N := count / limit
	if count%limit != 0 {
		N += 1
	}

	if N == 0 {
		metaFile.WriteString(fmt.Sprintf("No data for:%s->%s", dateFrom.ToString(), dateTo.ToString()))
		return nil, nil, nil
	}

	type PromoDataWorker struct {
		promodata []PromoData
		shopIds   map[int64]bool
		error     error
		filepath  string
	}
	sem := make(chan PromoDataWorker, N)

	filenum := 0
	for offset < count {

		go func(offset int, filenum int) {
			datafile := fmt.Sprintf("%s/data_%s_%s_%d", dataDirPath, dateFrom.ToString(), dateTo.ToString(), filenum)
			promodata, shopIds := getPromotionDataWithOffset(promo, from, to, limit, offset, db)
			err := writePromoDataToFile(promodata, datafile)
			log.Println("len:", len(promodata), len(shopIds))
			sem <- PromoDataWorker{promodata: promodata, shopIds: shopIds, error: err, filepath: datafile}
		}(offset, filenum)
		offset += limit
		filenum++
	}

	// wait for goroutines to finish
	var promoData []PromoData
	shopIdList := make(map[int64]bool)
	for i := 0; i < N; {
		i++
		promodataWorker := <-sem
		if promodataWorker.error != nil {
			_, err = metaFile.WriteString(fmt.Sprintf("Error while writing to file:%s, err:%v", promodataWorker.filepath, promodataWorker.error))
			if err != nil {
				log.Println("Couldn't write to meta itself with error:", err)
			}
		}
		promoData = append(promoData, promodataWorker.promodata...)
		updateShopListMap(promodataWorker.shopIds, shopIdList)
	}

	shopSellerMap, err := getShopSellerMap(shopIdList, db, fmt.Sprintf("%s/shop_seller_map", dataDirPath))
	fmt.Println("TotalShopSellerLength:", len(shopSellerMap))

	return promoData, shopSellerMap, nil

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

// Will write the promo data in CSV ( shippingRef, buyer id,shop id)
func writePromoDataToFile(promoData []PromoData, filepath string) error {

	f, err := os.Create(filepath)
	if err != nil {
		log.Println("Error while creating file:", filepath, err)
		return err
	}
	defer f.Close()

	for _, pd := range promoData {
		stmt := fmt.Sprintf("%s,%d,%d\n", pd.shippingRefNumber, pd.buyerId, pd.shopId)
		if _, err := f.WriteString(stmt); err != nil {
			log.Println("Error while writing to datafile:", err)
			return err
		}
	}
	return nil
}

func getShopSellerMap(shopIdList map[int64]bool, db *sql.DB, shopSellerMapFile string) (map[int64]int64, error) {
	var shopSellerMap map[int64]int64
	//shopSellerMap := make(map[int64]int64)

	if _, err := os.Stat(shopSellerMapFile); os.IsNotExist(err) {
		shopSellerMap = make(map[int64]int64)
	} else {
		shopSellerMap, err = GetShopSellerMap(shopSellerMapFile)
		if err != nil {
			log.Println("Error while reading existing shopSellerMap File:", err)
			return nil, err
		}
	}

	//Updating shopIdList
	for k, _ := range shopIdList {
		if shopSellerMap[k] != 0 {
			delete(shopIdList, k)
		}
	}

	//No new entry
	if len(shopIdList) == 0 {
		return shopSellerMap, nil
	}

	f, err := os.OpenFile(shopSellerMapFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Error while creating/writing to shopSellerMapFile:", shopSellerMapFile, err)

	}
	defer f.Close()

	sellerList := utils.HashSetToCSV(shopIdList)
	query := fmt.Sprintf(`SELECT shop_id,user_id FROM ws_shop WHERE shop_id in (%s)`, sellerList)

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return shopSellerMap, err
	}
	defer rows.Close()

	for rows.Next() {
		var shopId, sellerId int64
		if err = rows.Scan(&shopId, &sellerId); err != nil {
			log.Println(err)
			continue
		}
		if _, err := f.Write([]byte(fmt.Sprintf("%d,%d\n", shopId, sellerId))); err != nil {
			log.Println("Error while writing to shopSellerMapFile:", err)
			continue
		}
		shopSellerMap[shopId] = sellerId
	}
	return shopSellerMap, nil
}

func getOrderDataByPaymentIDs(paymentIds []int64, db *sql.DB) ([]PromoData, map[int64]bool) {
	defer utils.PrintTimeElapsed(time.Now(), "getOrderDataByPaymentIDs elapsedTime:")
	paymentIdsStr := utils.SliceToCSV(paymentIds)
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
