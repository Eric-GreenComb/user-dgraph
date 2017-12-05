package promotion

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	//"github.com/dgraph-io/dgraph/protos"
	"github.com/tokopedia/user-dgraph/dgraph"
	"github.com/tokopedia/user-dgraph/utils"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
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

	uniqueUserListFile := dirpath + "/unique_user_ids"
	if _, err := os.Stat(uniqueUserListFile); os.IsNotExist(err) {
		msg := fmt.Sprintf("uniqueUserListFile file doesn't exist cant process,path:%s", uniqueUserListFile)
		log.Println(msg)
		return errors.New(msg)
	}

	uniqueUsersList, err := utils.GetStringListFromFile(uniqueUserListFile)
	if err != nil {
		return err
	}

	uniqueSRNsListFile := dirpath + "/unique_srns"
	if _, err := os.Stat(uniqueSRNsListFile); os.IsNotExist(err) {
		msg := fmt.Sprintf("uniqueSRNsListFile file doesn't exist cant process,path:%s", uniqueSRNsListFile)
		log.Println(msg)
		return errors.New(msg)
	}

	uniqueSrnsList, err := utils.GetStringListFromFile(uniqueSRNsListFile)
	if err != nil {
		return err
	}

	c := dgraph.GetClient()
	//First get all existing users
	starttime := time.Now()
	log.Println("Getting existing users at:", starttime)
	userUIDMap := make(map[int64]string)
	var fromidx int
	for {
		lastslice := false
		toidx := fromidx + dgraph.QueryThreshold

		if toidx >= len(uniqueUsersList) {
			lastslice = true
			toidx = len(uniqueUsersList)
		}

		useridscsv := utils.StringListToCSVString(uniqueUsersList[fromidx:toidx])
		usersuids, err := GetUsersUIDs(useridscsv, c)
		if err != nil {
			return err
		}
		for _, useruid := range usersuids {
			userUIDMap[useruid.Userid] = useruid.Uid
		}

		if lastslice {
			break
		}
		fromidx = toidx
	}
	log.Println("Getting existing user time spent:", utils.GetTimeElapsed(starttime))
	//Create remaining users
	/*lenuserchan := len(uniqueUsersList) - len(userUIDMap)
	type UserIdUid struct {
		UserId int64
		Uid    string
	}
	uidchan := make(chan UserIdUid, lenuserchan)
	errchan := make(chan error, lenuserchan)*/
	starttime = time.Now()
	log.Println("UserCreation started at:", starttime)
	counter := 0
	for _, userid := range uniqueUsersList {
		useridi, _ := strconv.ParseInt(userid, 10, 64)
		if userUIDMap[useridi] == "" {
			//go func(userid int64) {
			uid, err := CreateUser(useridi, c)
			//	errchan <- err
			//	uidchan <- UserIdUid{useridi, uid}
			//}(useridi)

			if err != nil {
				return err
			}
			userUIDMap[useridi] = uid
			counter++
			//log.Println(fmt.Sprintf("UserCreated:%d->%s", useridi, uid))
		}
	}
	log.Println(fmt.Sprintf("Total user created (%d) with time spent:(%v)", counter, utils.GetTimeElapsed(starttime)))

	/*for i := 0; i < 2*lenuserchan; i++ {
		select {
		case err := <-errchan:
			if err != nil {
				return err
			}
		case uids := <-uidchan:
			if uids.Uid != "" && uids.UserId != 0 {
				userUIDMap[uids.UserId] = uids.Uid
			}
		}
	}*/

	starttime = time.Now()
	log.Println("Getting existing srns started at:", starttime)
	//First get all existing srns
	srnUIDmap := make(map[string]string)
	fromidx = 0
	for {
		lastslice := false
		toidx := fromidx + dgraph.QueryThreshold

		if toidx >= len(uniqueSrnsList) {
			lastslice = true
			toidx = len(uniqueSrnsList)
		}

		srnscsv := utils.StringListToCSVString(uniqueSrnsList[fromidx:toidx])
		srnUids, err := GetSRNUIDs(srnscsv, c)
		if err != nil {
			return err
		}
		for _, srnuid := range srnUids {
			srnUIDmap[srnuid.ShipRefNum] = srnuid.Uid
		}
		if lastslice {
			break
		}
		fromidx = toidx
	}
	log.Println("Getting existing srns time spent:", utils.GetTimeElapsed(starttime))

	//Create remaining srns
	counter = 0
	starttime = time.Now()
	log.Println("SRNCreation started at:", starttime)
	for _, srn := range uniqueSrnsList {
		if srnUIDmap[srn] == "" {
			uid, err := CreateSRN(srn, c)
			if err != nil {
				return err
			}
			srnUIDmap[srn] = uid
			counter++
			//log.Println(fmt.Sprintf("SRNCreated:%s>%s", srn, uid))
		}
	}
	log.Println(fmt.Sprintf("Total srns created (%d) with time spent:%v", counter, utils.GetTimeElapsed(starttime)))

	//Relationship creation started
	starttime = time.Now()
	log.Println("Relationship creation started at:", starttime)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "data") && !processedFiles[f.Name()] {
			log.Println("Processing:", dirpath+"/"+f.Name())
			//go func(path string) {
			promoData, err := ReadPromoData(dirpath+"/"+f.Name(), shopSellerMap)
			if err != nil {
				log.Println("Error while reading from file:", dirpath+"/"+f.Name(), err)
			}
			err = CreateRelationships(promoData, shopSellerMap, userUIDMap, srnUIDmap, c)
			if err != nil {
				log.Println("Err:", err)
			} else {
				logfile.WriteString(fmt.Sprintf("%s\n", f.Name()))
			}
			//}(dirpath + "/" + f.Name())
		}
	}
	log.Println("All relationship created, time spent:", utils.GetTimeElapsed(starttime))

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

func CreateRelationships(promoDataList []PromoData, shopSellerMap map[int64]int64, userUidMap map[int64]string, srnUidMap map[string]string, c *client.Dgraph) error {
	starttime := time.Now()
	ctx := context.Background()
	txn := c.NewTxn()
	defer txn.Discard(ctx)

	//err := c.Alter(ctx, &protos.Operation{DropAll: true})

	//var (
	//	mutex sync.Mutex
	//	wg    sync.WaitGroup
	//)
	//Write PromoData
	counter := 0
	for _, promoData := range promoDataList {
		//wg.Add(1)

		srnUid := srnUidMap[promoData.shippingRefNumber]
		buyerUid := userUidMap[promoData.buyerId]
		sellerUid := userUidMap[shopSellerMap[promoData.shopId]]

		//go func(srn, buyerUid, sellerUid string, c *client.Dgraph) {
		//	defer wg.Done()
		err := CreateRelation(srnUid, buyerUid, sellerUid, c)
		if err != nil {
			return err
		}
		counter++
		//}(srn, buyerUid, sellerUid, c)
	}
	//wg.Wait()
	log.Println(fmt.Sprintf("Total relationships created (%d) with time spent:%v", counter, utils.GetTimeElapsed(starttime)))
	return nil
}
