package promotion

import (
	"fmt"
	_ "github.com/lib/pq"
	"github.com/tokopedia/user-dgraph/utils"
	"log"
	"os"
	"testing"
	"time"
)

func TestGetPaymentRefs(t *testing.T) {

	promo_ship_refs, err := os.Create(fmt.Sprintf("/Users/ajayk/Documents/dgraph/promo_ship_ref_%v", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer promo_ship_refs.Close()

	//promo_ship_refs.WriteString("HEHE/n")
	//promo_ship_refs.Sync()

	from := Date{2017, time.November, 9}
	to := Date{2017, time.November, 12}
	promo_ship_refs.WriteString(fmt.Sprintf("%v : %v\n", from, to))

	//promoData, shopSellerMap, err := GetPromotionData(from, to, "CASHBACKPASTI", "", nil)
	//promo_ship_refs.WriteString(fmt.Sprintf("Total PromoData:%d\n", len(promoData)))

	//WritetoDgraph(promoData, shopSellerMap)

	promo_ship_refs.Sync()
}

func TestSliceToCSV(t *testing.T) {
	//fmt.Println(fmt.Sprintf("/Users/ajayk/Documents/dgraph/promo_ship_ref_%v", time.Now().UnixNano()))
	starttime := time.Now().Add(-100000)
	log.Println(fmt.Sprintf("Total user created (%d) with time spent:(%s)", 10, utils.GetTimeElapsed(starttime)))
	/*amap := make(map[string]string)
	amap["a"] = "z"
	amap["b"] = "y"
	amap["c"] = "x"
	amap["d"] = "w"
	amap["e"] = "v"

	log.Println(amap)
	//for k, v := range amap {
	//	log.Println(k, v)
	//}
	log.Println(amap)*/
}

func updatemap(amap map[int64]int64) {
	amap[5] = 130
	return
}
