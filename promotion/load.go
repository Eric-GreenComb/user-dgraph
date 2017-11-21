package promotion

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
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

	for _, f := range files {
		if strings.HasPrefix(f.Name(), "data") && !processedFiles[f.Name()] {
			log.Println("Processing:", dirpath+"/"+f.Name())
			//go func(path string) {
			promoData, err := ReadPromoData(dirpath+"/"+f.Name(), shopSellerMap)
			if err != nil {
				log.Println("Error while reading from file:", dirpath+"/"+f.Name(), err)
			}
			err = WriteToDgraphV2(promoData, shopSellerMap, dgraph.NewClient())
			if err != nil {
				log.Println("Err:", err)
			} else {
				logfile.WriteString(fmt.Sprintf("%s\n", f.Name()))
			}
			//WritetoDgraph(promoData, shopSellerMap)

			//}(dirpath + "/" + f.Name())
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

func getUsers(promodatalist []PromoData, shopSellerMap map[int64]int64) map[int64]bool {

	users := make(map[int64]bool)

	for _, v := range promodatalist {
		users[v.buyerId] = true
		users[shopSellerMap[v.shopId]] = true
	}
	return users
}

func DropAll(c *client.Dgraph) error {
	err := c.Alter(context.Background(), &protos.Operation{DropAll: true})
	if err != nil {
		log.Println("Error while DropAll:", err)
	}
	return err
}

func WriteToDgraphV2(promoDataList []PromoData, shopSellerMap map[int64]int64, c *client.Dgraph) error {
	defer utils.PrintTimeElapsed(time.Now(), "WriteToDgraphV2 Elapsed:")
	ctx := context.Background()
	txn := c.NewTxn()
	defer txn.Discard(ctx)

	//err := c.Alter(ctx, &protos.Operation{DropAll: true})

	users := getUsers(promoDataList, shopSellerMap)
	usersStr := utils.HashSetIntToCSVString(users)
	u_q := fmt.Sprintf(`{
			get_users(func: eq(user_id, [%s])){
				uid
				user_id
			}
		}`, usersStr)

	resp, err := txn.Query(ctx, u_q)
	if err != nil {
		log.Println(u_q, err)
		return err
	}
	var usersDecode struct {
		GetUsers []struct {
			Uid    string `json:"uid"`
			Userid int64  `json:"user_id"`
		} `json:"get_users"`
	}

	if err := json.Unmarshal(resp.GetJson(), &usersDecode); err != nil {
		log.Println(resp, err)
	}

	//1.
	useridUidMap := make(map[int64]string)
	for _, v := range usersDecode.GetUsers {
		useridUidMap[v.Userid] = v.Uid
	}

	//Create remaining users
	for userid := range users {
		if useridUidMap[userid] == "" {
			u := UserDgraph{
				Name:   "USER",
				UserId: userid,
			}
			mu := &protos.Mutation{CommitNow: true}
			ujson, err := json.Marshal(u)
			if err != nil {
				log.Println("Marshal error:", u, err)
				return err
			}
			mu.SetJson = ujson
			assigned, err := c.NewTxn().Mutate(ctx, mu)
			if err != nil {
				log.Println("Dgraph user creation error", mu, err)
				return err
			}
			userUid := assigned.Uids["blank-0"]
			useridUidMap[userid] = userUid
		}
	}

	shipRefNumSet := getShipRefNums(promoDataList)
	shipRefNumSetStr := utils.HashSetStringToCSVString(shipRefNumSet)
	srn_q := fmt.Sprintf(`{
			get_srns(func: eq(ship_ref_num, [%s])){
				uid
				ship_ref_num
			}
		}`, shipRefNumSetStr)

	resp, err = txn.Query(ctx, srn_q)
	if err != nil {
		log.Println(srn_q, err)
		return err
	}

	var srnsDecode struct {
		GetSrns []struct {
			Uid        string `json:"uid"`
			ShipRefNum string `json:"ship_ref_num"`
		} `json:"get_srns"`
	}

	if err := json.Unmarshal(resp.GetJson(), &srnsDecode); err != nil {
		log.Println(resp, err)
	}

	//2.
	srnsUidMap := make(map[string]string)
	for _, v := range srnsDecode.GetSrns {
		srnsUidMap[v.ShipRefNum] = v.Uid
	}

	//var (
	//	mutex sync.Mutex
	//	wg    sync.WaitGroup
	//)
	//Write PromoData
	for _, promoData := range promoDataList {
		//wg.Add(1)

		srn := promoData.shippingRefNumber
		buyerUid := useridUidMap[promoData.buyerId]
		sellerUid := useridUidMap[shopSellerMap[promoData.shopId]]

		//TODO: have to find a way to pass the error from go routine
		//go func(srn, buyerUid, sellerUid string, c *client.Dgraph) {
		//	defer wg.Done()
		ctx := context.Background()

		srnUid := srnsUidMap[promoData.shippingRefNumber]

		s := ShipRefNumDgraph{
			Name:       "ShippingRefNumber",
			ShipRefNum: srn,
			Uid:        srnUid,
			Buyer:      UserDgraph{Uid: buyerUid},
			Seller:     UserDgraph{Uid: sellerUid},
		}
		mu := &protos.Mutation{CommitNow: true}
		srnjson, err := json.Marshal(s)
		if err != nil {
			log.Println("Marshal Error", s, err)
		}
		mu.SetJson = srnjson
		txn := c.NewTxn()
		defer txn.Discard(ctx)

		assigned, err := txn.Mutate(ctx, mu)
		if err != nil {
			log.Println("Dgraph srn creation error:", mu, err)
			return err
		}

		if srnUid == "" {
			newSrnUid := assigned.Uids["blank-0"]
			//Concurrent map write
			//mutex.Lock()
			srnsUidMap[srn] = newSrnUid
			//mutex.Unlock()
		}

		//}(srn, buyerUid, sellerUid, c)
	}
	//wg.Wait()

	return nil
}

type UserDgraph struct {
	Uid    string `json:"uid,omitempty"`
	Name   string `json:"name,omitempty"`
	UserId int64  `json:"user_id,omitempty"`
}

type ShipRefNumDgraph struct {
	Uid        string     `json:"uid,omitempty"`
	Name       string     `json:"name,omitempty"`
	ShipRefNum string     `json:"ship_ref_num, omitempty"`
	Buyer      UserDgraph `json:"buyer,omitempty"`
	Seller     UserDgraph `json:"seller,omitempty"`
}

func getShipRefNums(list []PromoData) map[string]bool {
	shipRefNumSet := make(map[string]bool)
	for _, promodata := range list {
		shipRefNumSet[promodata.shippingRefNumber] = true
	}
	return shipRefNumSet
}

func WritetoDgraph(promoDataList []PromoData, shopSellerMap map[int64]int64) {
	//defer logfile.WriteString(fmt.Sprintf("Total time spent in dgraph writing:%v", utils.GetTimeElapsed(time.Now())))

	query :=
		`{
			buyer  as var(func: eq(user_id, "%v"))      @upsert
			seller as var(func: eq(user_id, "%v"))      @upsert
			s_r    as var(func: eq(ship_ref_num, "%v")) @upsert
		}

		mutation {
		  set {
			uid(s_r) <name> "ShippingRefNumber" .
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

		//logfile.WriteString(promoData.shippingRefNumber + "\n")

		dgraph.UpsertDgraph(fmt.Sprintf(query, buyer, seller, shipRefNum))

	}
}
