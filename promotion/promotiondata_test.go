package promotion

import (
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"testing"
	"time"
)

func TestGetPaymentRefs(t *testing.T) {

	promo_ship_refs, err := os.Create("/Users/ajayk/Documents/dgraph/promo_ship_ref" + time.Now().String())
	if err != nil {
		panic(err)
	}
	defer promo_ship_refs.Close()

	//promo_ship_refs.WriteString("HEHE/n")
	//promo_ship_refs.Sync()

	from := Date{2017, time.November, 9}
	to := Date{2017, time.November, 12}
	promo_ship_refs.WriteString(fmt.Sprintf("%v : %v\n", from, to))

	promoData, shopSellerMap := GetPromotionData("CASHBACKPASTI", from, to)
	promo_ship_refs.WriteString(fmt.Sprintf("Total PromoData:%d\n", len(promoData)))

	WritetoDgraph(promoData, shopSellerMap, promo_ship_refs)

	promo_ship_refs.Sync()
}

func TestSliceToCSV(t *testing.T) {
	fmt.Println(fmt.Sprintf("%v", (Date{2017, time.November, 9})))

}
