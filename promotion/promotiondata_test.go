package promotion

import (
	"fmt"
	_ "github.com/lib/pq"
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
	frm := Date{2017, 11, 11}
	nextDay := GetDate(frm.GetTime().AddDate(0, 0, 1))
	fmt.Println(frm.GetTime().Format(time.RFC3339))
	fmt.Println(nextDay.GetTime().Format(time.RFC3339))
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
