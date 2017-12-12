package main

import (
	"log"
	"net/http"
	"os"

	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/tokopedia/user-dgraph/dgraph"
	"github.com/tokopedia/user-dgraph/promotion"
	"github.com/tokopedia/user-dgraph/riderorder"
	"github.com/tokopedia/user-dgraph/userlogin"
	"io/ioutil"

	"context"
	"github.com/tokopedia/user-dgraph/utils"
	"time"
)

func main() {
	port := os.Getenv("PORT")

	if port == "" {
		log.Println("$PORT should be set")
		port = "5000"
	}

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/static", "static")

	router.StaticFile("/manifest.json", "manifest.json")

	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl.html", nil)
	})

	router.POST("/dgraph/push/rider-order", func(ginContext *gin.Context) {
		var obj riderorder.DynamoStreamRecord
		err := ginContext.BindJSON(&obj)
		if err != nil {
			ginContext.JSON(http.StatusBadRequest, `{'result':'invalid_req'}`)
		} else {
			ctx := context.Background()
			riderorder.LoadRideData(ctx, &obj)
			ginContext.JSON(200, `{'result':'ok'}`)
		}

	})
	router.POST("/dgraph/push/user-login", func(ginContext *gin.Context) {
		var req []byte
		req, err := ioutil.ReadAll(ginContext.Request.Body)
		if err != nil {
			log.Println("fail to read request data")
			return
		}

		ctx := context.Background()
		userlogin.LoadUserLoginData(ctx, req)
		ginContext.JSON(200, "{'result':'ok'}")

	})

	router.POST("/dgraph/get-promodata", func(context *gin.Context) {
		var requestobj promotion.PromoDataRequest
		context.BindJSON(&requestobj)
		log.Println("Sync Req:", requestobj)
		if !requestobj.From.IsValid() || !requestobj.To.IsValid() || requestobj.Promocode == "" {
			context.JSON(http.StatusBadRequest, "{'result':'invalid_req'}")
		} else {

			dirname, dirpath, err := promotion.GetProcessingDir(requestobj)
			if err != nil {
				log.Println("Couldn't create the data directory with error:", err)
				context.JSON(http.StatusInternalServerError, fmt.Sprintf("{'result':'ok', 'error':'%v'}", err))
			}

			context.JSON(200, fmt.Sprintf("{'result':'ok', 'dir':'%s'}", dirname))
			err = promotion.Process(requestobj, dirpath)
			if err != nil {
				log.Println("Error /dgraph/get-promodata:", err)
			}
		}
	})

	router.POST("/dgraph/load-promodata", func(context *gin.Context) {
		var requestObj promotion.LoadDataRequest
		context.BindJSON(&requestObj)
		log.Println("Load Promo Req:", requestObj)
		if requestObj.Dirname == "" {
			context.JSON(http.StatusBadRequest, "{'result':'invalid_req'}")
		} else {
			context.JSON(200, fmt.Sprintf("{'result':'ok'}"))
			err := promotion.LoadDgraph(requestObj.Dirname)
			if err != nil {
				log.Println("Error /dgraph/load-promodata:", err)
			}

		}
	})

	router.POST("/restricted/dropall", func(context *gin.Context) {
		err := dgraph.DropAll()
		if err != nil {
			context.JSON(512, fmt.Sprintf("{'result':'failed', 'message':'%v'}", err))
		} else {
			context.JSON(200, "{'result':'ok'}")
		}
	})

	router.GET("/dgraph/_status", func(context *gin.Context) {
		status := dgraph.CheckHealth()
		if status != "OK" {
			context.JSON(512, fmt.Sprintf("{'result':'failed', 'message':'%v'}", status))
		} else {
			context.JSON(200, "{'result':'ok'}")
		}
	})

	router.POST("/restricted/loadtest", func(context *gin.Context) {
		type req struct {
			OuterLoop    int `json:"outer_loop"`
			ParallelExec int `json:"parallel_exec"`
		}

		var requestObj req
		context.BindJSON(&requestObj)

		log.Println("Load loadtest Req:", requestObj)

		if requestObj.OuterLoop == 0 || requestObj.ParallelExec == 0 {
			context.JSON(http.StatusBadRequest, "{'result':'invalid_req'}")
		} else {
			go TestingLoad(requestObj.OuterLoop, requestObj.ParallelExec)
			context.JSON(200, "{'result':'ok'}")
		}
	})

	router.Run(":" + port)
}

const (
	KEYS      = "KEYS"
	OLD_IMAGE = "OldImage"
	NEW_IMAGE = "NewImage"
)

func TestingLoad(outerloop, parallelexecs int) {
	defer utils.PrintTimeElapsed(time.Now(), "Total time spent:")
	ctx := context.Background()
	cl := dgraph.GetClient()

	for i := 0; i < outerloop; i++ {
		c := make(chan bool, parallelexecs)
		for j := 0; j < parallelexecs; j++ {
			go func(i, j int) {
				log.Println("Done from:", i, j)
				t := time.Now()
				q := fmt.Sprintf(`
						_:luke <name> "Luke Skywalker_%v" .
						_:sw1 <name> "Star Wars: Episode IV - A New Hope_%v" .
						_:sw1 <starring> _:luke .
					`, t, t)

				err := dgraph.RetryMutate(ctx, cl, q, 1)
				if err != nil {
					log.Fatal(err)
				}
				c <- true
			}(i, j)
		}

		for j := 0; j < parallelexecs; j++ {
			<-c
		}
		time.Sleep(10 * time.Millisecond)
		log.Println("All done")
	}
}
