package main

import (
	"log"
	"net/http"
	"os"

	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/tokopedia/user-dgraph/promotion"
	"github.com/tokopedia/user-dgraph/riderorder"
	"github.com/tokopedia/user-dgraph/userlogin"
	"io/ioutil"
	"github.com/tokopedia/user-dgraph/dgraph"
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

	router.POST("/dgraph/push/rider-order", func(context *gin.Context) {
		var obj riderorder.DynamoStreamRecord
		context.BindJSON(&obj)
		log.Println("RiderOrder->>>>>>", obj)
		riderorder.LoadRideData(&obj)
		context.JSON(200, `{'result':'ok'}`)

	})
	router.POST("/dgraph/push/user-login", func(context *gin.Context) {
		var req []byte
		req, err := ioutil.ReadAll(context.Request.Body)
		if err != nil {
			log.Println("fail to read request data")
			return
		}

		//var obj userlogin.DynamoStreamRecord
		//context.BindJSON(&obj)
		//fmt.Println("UserLogin->>>>>>", obj)
		userlogin.LoadUserLoginData(req)
		context.JSON(200, "{'result':'ok'}")

	})

	router.POST("/dgraph/sync-promodata", func(context *gin.Context) {
		var requestobj promotion.PromoDataRequest
		context.BindJSON(&requestobj)
		log.Println("Sync Req:", requestobj)
		if !requestobj.From.IsValid() || !requestobj.To.IsValid() || requestobj.Promocode == "" {
			context.JSON(200, "{'result':'invalid_req'}")
		} else {
			promotion.Process(requestobj)
			context.JSON(200, "{'result':'ok'}")
		}
	})

	router.GET("/dgraph/_status", func(context *gin.Context) {
		status := dgraph.CheckHealth()
		if status != "OK" {
			context.JSON(512, fmt.Sprintf("{'result':'failed', 'message':'%v'}",status))
		}else{
			context.JSON(200, "{'result':'ok'}")
		}
	})

	router.Run(":" + port)
}

type UserLoginDynamoStreamRecord struct {
	Keys map[string]map[string]string `json:"Keys"`
}

const (
	KEYS      = "KEYS"
	OLD_IMAGE = "OldImage"
	NEW_IMAGE = "NewImage"
)

func TestUserLogin() {
	filepath := "/Users/ajayk/Downloads/sample_user_login.json"
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		log.Println("Couldn't read file", err)
	}
	userlogin.LoadUserLoginData(data)
}
