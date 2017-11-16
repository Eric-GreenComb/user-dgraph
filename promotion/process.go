package promotion

import (
	"os"
	"time"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
)

func Process(request PromoDataRequest) {
	logdir := utils.CreateLogDirectory()
	promo_ship_refs, err := os.Create(logdir +"/promo_ship_ref" + time.Now().String())
	if err != nil {
		panic(err)
	}
	defer promo_ship_refs.Close()


	from := request.From
	to := request.To
	promo_ship_refs.WriteString(fmt.Sprintf("%v : %v\n", from, to))

	promoData, shopSellerMap := GetPromotionData("CASHBACKPASTI", from, to)
	promo_ship_refs.WriteString(fmt.Sprintf("Total PromoData:%d\n", len(promoData)))

	WritetoDgraph(promoData, shopSellerMap, promo_ship_refs)

	promo_ship_refs.Sync()
}

