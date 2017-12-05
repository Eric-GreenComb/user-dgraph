package dgraphmodels

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"log"
)

type GetUser struct {
	Uid    string `json:"uid"`
	Userid int64  `json:"user_id"`
}

type UserDGraph struct {
	Uid          string              `json:"uid,omitempty"`
	Name         string              `json:"name,omitempty"`
	UserId       int64               `json:"user_id,omitempty"`
	Fingerprints []FingerprintDGraph `json:"DeviceFingerPrint,omitempty"`
}

func GetUsersUIDs(useridscsv string, c *client.Dgraph) ([]GetUser, error) {
	ctx := context.Background()
	txn := c.NewTxn()
	defer txn.Discard(ctx)

	u_q := fmt.Sprintf(`{
			get_users(func: eq(user_id, [%s])){
				uid
				user_id
			}
		}`, useridscsv)

	resp, err := txn.Query(ctx, u_q)
	if err != nil {
		log.Println(u_q, err)
		return nil, err
	}

	var usersDecode struct {
		GetUsers []GetUser `json:"get_users"`
	}

	if err := json.Unmarshal(resp.GetJson(), &usersDecode); err != nil {
		log.Println(resp, err)
		return nil, err
	}
	return usersDecode.GetUsers, nil
}

//Returns uid of newly created user
func CreateUser(userid int64, c *client.Dgraph) (string, error) {
	ctx := context.Background()
	u := UserDGraph{
		Name:   "USER",
		UserId: userid,
	}
	mu := &protos.Mutation{CommitNow: true}
	ujson, err := json.Marshal(u)
	if err != nil {
		log.Println("Marshal error:", u, err)
		return "", err
	}
	mu.SetJson = ujson
	assigned, err := c.NewTxn().Mutate(ctx, mu)
	if err != nil {
		log.Println("Dgraph user creation error", mu, err)
		return "", err
	}
	return assigned.Uids["blank-0"], nil
}
