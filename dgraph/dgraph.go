package dgraph

import (
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"google.golang.org/grpc"
	"log"
)

var dgraphhost = "10.0.11.162:7080" //"localhost:9080"

func NewClient() *client.Dgraph {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	d, err := grpc.Dial(dgraphhost, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	return client.NewDgraphClient(
		protos.NewDgraphClient(d),
	)
}
