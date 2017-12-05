package dgraph

import (
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"
	"google.golang.org/grpc"
	"log"
	"strings"
	"time"
)

var dgraphhost = "10.255.151.17:7080" //<- DCI//"10.0.11.162:7080" //<-Aws //"localhost:9080"
const (
	QueryThreshold           = 10000
	DGraphMutationRetryCount = 20
)

var c *client.Dgraph

func newClient() *client.Dgraph {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	d, err := grpc.Dial(dgraphhost, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	return client.NewDgraphClient(
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
		api.NewDgraphClient(d),
	)
}

func GetClient() *client.Dgraph {
	if c == nil {
		c = newClient()
	}
	return c
}

func RetryMutate(ctx context.Context, cl *client.Dgraph, query string, counter int) error {
	totalCount := counter
	for counter > 0 {
		err := doMutate(ctx, cl, query)
		if err != nil {
			if strings.Contains(err.Error(), "Transaction aborted") {
				counter--
				time.Sleep(10 * time.Millisecond)
			} else {
				return err
			}
		} else {
			return nil
		}
	}
	if counter == 0 {
		return errors.New(fmt.Sprintf("Tried transaction commit for %d times but couldn't commit.", totalCount))
	}
	return nil
}

func doMutate(ctx context.Context, cl *client.Dgraph, query string) error {
	txn := cl.NewTxn()
	defer txn.Discard(ctx)

	mu := &api.Mutation{SetNquads: []byte(query)}
	_, err := txn.Mutate(ctx, mu)
	if err != nil {
		return err
	}
	err = txn.Commit(ctx)
	return err
}

func DropAll() error {
	c := GetClient()
	err := c.Alter(context.Background(), &api.Operation{DropAll: true})
	if err != nil {
		log.Println("Error while DropAll:", err)
	}
	return err
}
