package dgraph

import (
	"context"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"log"
	"strings"
)

var dgraphhost = "10.0.11.162:7080" //"localhost:9080"
const (
	QueryThreshold           = 10000
	DGraphMutationRetryCount = 10
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
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
		protos.NewDgraphClient(d),
	)
}

func GetClient() *client.Dgraph {
	if c == nil {
		c = newClient()
	}
	return c
}

func RetryMutate(ctx context.Context, cl *client.Dgraph, query string, counter int) error {
	for counter > 0 {
		err := doMutate(ctx, cl, query)
		if err != nil {
			if strings.Contains(err.Error(), "Transaction aborted") {
				counter--
			} else {
				return err
			}
		} else {
			return nil
		}
	}
	if counter == 0 {
		return errors.New(fmt.Sprintf("Tried transaction commit for %d times but couldn't commit.", counter))
	}
	return nil
}

func doMutate(ctx context.Context, cl *client.Dgraph, query string) error {
	txn := cl.NewTxn()
	defer txn.Discard(ctx)

	mu := &protos.Mutation{SetNquads: []byte(query)}
	_, err := txn.Mutate(ctx, mu)
	if err != nil {
		return err
	}
	err = txn.Commit(ctx)
	return err
}
