package promotion

import (
	"errors"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
	"log"
	"os"
	"time"
)

func GetProcessingDir(request PromoDataRequest) (string, string, error) {
	dataDirName := fmt.Sprintf("promodata_%s_%v", request.Promocode, time.Now().UnixNano())
	dataDirPath, err := utils.CreateDirInsideLogDir(dataDirName)
	return dataDirName, dataDirPath, err
}
func Process(request PromoDataRequest, dataDirPath string) error {
	utils.CreateLogDirectory()
	from := request.From
	to := request.To

	meta, err := os.Create(fmt.Sprintf("%s/meta", dataDirPath))
	if err != nil {
		return err
	}
	defer meta.Close()

	_, err = meta.WriteString(fmt.Sprintf("Promo:%s, %v -> %v\n", request.Promocode, from, to))
	if err != nil {
		log.Println(fmt.Sprintf("Couldn't write data to meta(%s/%s) with error:%v", dataDirPath, meta.Name(), err))
		return err
	}

	if from.Equal(to) {
		meta.WriteString("From/To are same\n")
		return errors.New("From/To are same")
	}

	uniqueUserIds := make(map[int64]bool)
	uniqueSrns := make(map[string]bool)
	//processed_srns.WriteString(fmt.Sprintf("%v : %v\n", from, to))
	nextDay := GetDate(from.GetTime().AddDate(0, 0, 1))
	//var shopSellerMap map[int64]int64

	for {

		promodata, shopSellerMap, err := GetPromotionData(from, nextDay, request.Promocode, dataDirPath, meta)
		updateUniqueDataByPromoData(uniqueUserIds, uniqueSrns, promodata)
		updateUniqueUserIdsByShopSellerMap(uniqueUserIds, shopSellerMap)
		if nextDay.Equal(to) || err != nil {
			if err != nil {
				log.Println("Got error while getting/storing data from, to:", from, nextDay, err)
			}
			break
		}
		from = nextDay
		nextDay = GetDate(from.GetTime().AddDate(0, 0, 1))

	}
	log.Println("Len uniqueUserIds:", len(uniqueUserIds))
	log.Println("Len uniqueSrns:", len(uniqueSrns))

	filenUniqueUsers := "unique_user_ids"
	err = utils.WriteInt64HashSetToFile(uniqueUserIds, dataDirPath, filenUniqueUsers)
	if err != nil {
		_, err = meta.WriteString(fmt.Sprintf("Error while writing unique users to file:%s, err:%v", filenUniqueUsers, err))
		if err != nil {
			log.Println(fmt.Sprintf("Couldn't write data to meta(%s/%s) with error:%v", dataDirPath, meta.Name(), err))
			return err
		}
	}

	filenUniqueSRNs := "unique_srns"
	err = utils.WriteStringHashSetToFile(uniqueSrns, dataDirPath, filenUniqueSRNs)
	if err != nil {
		_, err = meta.WriteString(fmt.Sprintf("Error while writing unique users to file:%s, err:%v", filenUniqueSRNs, err))
		if err != nil {
			log.Println(fmt.Sprintf("Couldn't write data to meta(%s/%s) with error:%v", dataDirPath, meta.Name(), err))
			return err
		}
	}
	//promoData, shopSellerMap, err := GetPromotionData("CASHBACKPASTI", from, to)
	//processed_srns.WriteString(fmt.Sprintf("Total PromoData:%d\n", len(promoData)))

	//processedSRNs := getProcessedShipRefNums(logdir, "promo_ship_ref")

	//WritetoDgraph(promoData, shopSellerMap, processed_srns)

	meta.Sync()

	return err
}

func updateUniqueDataByPromoData(uniqueUsers map[int64]bool, uniqueSrns map[string]bool, promoData []PromoData) {
	for _, promod := range promoData {
		uniqueUsers[promod.buyerId] = true
		uniqueSrns[promod.shippingRefNumber] = true
	}
}

func updateUniqueUserIdsByShopSellerMap(destination map[int64]bool, shopSellerMap map[int64]int64) {
	for _, v := range shopSellerMap {
		destination[v] = true
	}
}
