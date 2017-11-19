package promotion

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type LoadDataRequest struct {
	Dirname string `json:"dirname"`
}

func LoadData(dir string) error {
	dirpath := utils.GetLogDir() + "/" + dir

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

	shopSellerMapFile := dirpath + "/shop_seller_map"

	if _, err := os.Stat(shopSellerMapFile); os.IsNotExist(err) {
		msg := fmt.Sprintf("ShopSellerMap file doesn't exist cant process,path:%s", shopSellerMapFile)
		log.Println(msg)
		return errors.New(msg)
	}

	shopSellerMap, err := GetShopSellerMap(shopSellerMapFile)
	if err != nil {
		return err
	}

	for _, f := range files {
		if strings.HasPrefix(f.Name(), "data") && !processedFiles[f.Name()] {
			go func(path string) {
				promoData, err := ReadPromoData(path, shopSellerMap)
				log.Println(err)
				WritetoDgraph(promoData, shopSellerMap)
				logfile.WriteString(fmt.Sprintf("%s\n", f.Name()))
			}(dirpath+"/"+f.Name())
		}
	}

	return nil
}

func ReadPromoData(path string, shopSellerMap map[int64]int64) ([]PromoData, error) {
	var promoDataList []PromoData
	file, err := os.Open(path)
	if err != nil {
		return promoDataList, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		temp := strings.Split(scanner.Text(), ",")
		shipRefNum := temp[0]
		buyerId, _ := strconv.ParseInt(temp[1], 10, 64)
		shopId, _ := strconv.ParseInt(temp[2], 10, 64)
		sellerId := shopSellerMap[shopId]
		pmd := PromoData{shopId: shopId, sellerId: sellerId, shippingRefNumber: shipRefNum, buyerId: buyerId}
		promoDataList = append(promoDataList, pmd)
	}

	return promoDataList, nil
}

func GetShopSellerMap(path string) (map[int64]int64, error) {
	shopSellerMap := make(map[int64]int64)
	file, err := os.Open(path)
	if err != nil {
		return shopSellerMap, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		temp := strings.Split(scanner.Text(), ",")
		shopid, _ := strconv.ParseInt(temp[0], 10, 64)
		sellerid, _ := strconv.ParseInt(temp[1], 10, 64)
		shopSellerMap[shopid] = sellerid
	}
	return shopSellerMap, nil
}
