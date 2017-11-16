package promotion

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

func GetProcessingDir(request PromoDataRequest)(string, error){
	dataDirName := fmt.Sprintf("promodata_%s_%v", request.Promocode, time.Now().UnixNano())
	dataDirPath, err := utils.CreateDirInsideLogDir(dataDirName)
	return dataDirPath, err
}
func Process(request PromoDataRequest, dataDirPath string )  error {
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

	//processed_srns.WriteString(fmt.Sprintf("%v : %v\n", from, to))
	nextDay := GetDate(from.GetTime().AddDate(0, 0, 1))
	for {

		_, _, err = GetPromotionData(from, nextDay, request.Promocode, dataDirPath, meta)
		if nextDay.Equal(to) || err != nil {
			break
		}
		from = nextDay
		nextDay = GetDate(from.GetTime().AddDate(0, 0, 1))
	}

	//promoData, shopSellerMap, err := GetPromotionData("CASHBACKPASTI", from, to)
	//processed_srns.WriteString(fmt.Sprintf("Total PromoData:%d\n", len(promoData)))

	//processedSRNs := getProcessedShipRefNums(logdir, "promo_ship_ref")

	//WritetoDgraph(promoData, shopSellerMap, processed_srns)

	meta.Sync()

	return err
}

func getProcessedShipRefNums(dir, prefix string) map[string]bool {

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("Couldn't open the directory:", err)
	}

	processedSRNs := make(map[string]bool)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix) {
			file, err := os.Open(fmt.Sprintf("%s/%s", dir, f.Name()))
			if err != nil {
				log.Println("Got error while opening file:", f.Name())
			} else {
				scanner := bufio.NewScanner(file)
				count := 0
				for scanner.Scan() {
					//skipping initial two lines
					if count > 1 {
						processedSRNs[scanner.Text()] = true
					}
					count++
				}
			}
		}
	}
	return processedSRNs
}
