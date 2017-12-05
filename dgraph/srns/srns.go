package srns

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"github.com/tokopedia/user-dgraph/dgraph/users"
	"log"
)

type GetSRN struct {
	Uid        string `json:"uid"`
	ShipRefNum string `json:"ship_ref_num"`
}
type DGraphModel struct {
	Uid        string            `json:"uid,omitempty"`
	Name       string            `json:"name,omitempty"`
	ShipRefNum string            `json:"ship_ref_num,omitempty"`
	Buyer      users.DGraphModel `json:"buyer,omitempty"`
	Seller     users.DGraphModel `json:"seller,omitempty"`
}

func GetSRNUIDs(srnscsv string, c *client.Dgraph) ([]GetSRN, error) {
	ctx := context.Background()
	txn := c.NewTxn()
	defer txn.Discard(ctx)

	srn_q := fmt.Sprintf(`{
			get_srns(func: eq(ship_ref_num, [%s])){
				uid
				ship_ref_num
			}
		}`, srnscsv)

	resp, err := txn.Query(ctx, srn_q)
	if err != nil {
		log.Println(srn_q, err)
		return nil, err
	}
	var srnDecode struct {
		GetSRNs []GetSRN `json:"get_srns"`
	}

	if err := json.Unmarshal(resp.GetJson(), &srnDecode); err != nil {
		log.Println(resp, err)
		return nil, err
	}
	return srnDecode.GetSRNs, nil
}

//Returns uid of newly created SRN
func CreateSRN(srn string, c *client.Dgraph) (string, error) {
	ctx := context.Background()
	srndgraph := DGraphModel{
		Name:       "ShippingRefNumber",
		ShipRefNum: srn,
	}
	mu := &protos.Mutation{CommitNow: true}
	srndjson, err := json.Marshal(srndgraph)
	if err != nil {
		log.Println("Marshal error:", srndgraph, err)
		return "", err
	}
	mu.SetJson = srndjson
	assigned, err := c.NewTxn().Mutate(ctx, mu)
	if err != nil {
		log.Println("Dgraph SRN creation error", mu, err)
		return "", err
	}
	return assigned.Uids["blank-0"], nil
}

func CreateRelation(srnuid, buyeruid, selleruid string, c *client.Dgraph) error {
	ctx := context.Background()
	s := DGraphModel{
		Uid:    srnuid,
		Buyer:  users.DGraphModel{Uid: buyeruid},
		Seller: users.DGraphModel{Uid: selleruid},
	}
	mu := &protos.Mutation{CommitNow: true}
	srnjson, err := json.Marshal(s)
	if err != nil {
		log.Println("Marshal Error", s, err)
	}
	mu.SetJson = srnjson
	_, err = c.NewTxn().Mutate(ctx, mu)
	if err != nil {
		log.Println("Dgraph SRN creation error", mu, err)
		return err
	}

	//_ := assigned.Uids["blank-0"]

	/*if newsrnuid != srnuid {
		msg := fmt.Sprintf("-------------->Wrongly created a new node which was not expected, oldsrn:%s, newsrn%s", srnuid, newsrnuid)
		log.Println(msg)
		return errors.New(msg)
	}*/
	return nil
}
