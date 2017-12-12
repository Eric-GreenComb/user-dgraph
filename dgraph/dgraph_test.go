package dgraph

import (
	"context"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
	"log"
	"testing"
	"time"
)

func TestRetryMutate(t *testing.T) {
	defer utils.PrintTimeElapsed(time.Now(), "Total time spent:")
	ctx := context.Background()
	cl := GetClient()

	for i := 0; i < 2000; i++ {
		c := make(chan bool, 10)
		for j := 0; j < 15; j++ {
			go func(i, j int) {
				log.Println("Done from:", i, j)
				t := time.Now()
				q := fmt.Sprintf(`
						_:luke <name> "Luke Skywalker_%v" .
						_:sw1 <name> "Star Wars: Episode IV - A New Hope_%v" .
						_:sw1 <starring> _:luke .
					`, t, t)

				err := doMutate(ctx, cl, q)
				if err != nil {
					log.Fatal(err)
				}
				c <- true
			}(i, j)
		}

		for j := 0; j < 10; j++ {
			<-c
		}
		time.Sleep(time.Second)
		log.Println("All done")
	}
}
