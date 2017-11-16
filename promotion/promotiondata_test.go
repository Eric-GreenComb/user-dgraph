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

	promoData, shopSellerMap, err := GetPromotionData("CASHBACKPASTI", from, to)
	promo_ship_refs.WriteString(fmt.Sprintf("Total PromoData:%d\n", len(promoData)))

	WritetoDgraph(promoData, shopSellerMap, promo_ship_refs)

	promo_ship_refs.Sync()
}

func TestSliceToCSV(t *testing.T) {
	//fmt.Println(fmt.Sprintf("/Users/ajayk/Documents/dgraph/promo_ship_ref_%v", time.Now().UnixNano()))
	list := getProcessedShipRefNums("/Users/ajayk/go/src/github.com/tokopedia/user-dgraph/logs", "promo_ship_ref")
	fmt.Println()
}
