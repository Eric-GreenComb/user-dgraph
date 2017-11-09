package main

import (
	"log"
	"net/http"
	"os"

	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"github.com/tokopedia/user-dgraph/riderorder"
	"github.com/tokopedia/user-dgraph/userlogin"
)

func main() {
	port := os.Getenv("PORT")

	if port == "" {
		//log.Fatal("$PORT must be set")
		log.Println("$PORT must be set")
		port = "5000"
	}

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/static", "static")

	router.StaticFile("/manifest.json", "manifest.json")
	//router.StaticFile("/OneSignalSDKUpdaterWorker.js", "OneSignalSDKUpdaterWorker.js")
	//router.StaticFile("/OneSignalSDKWorker.js", "OneSignalSDKWorker.js")

	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl.html", nil)
	})

	router.POST("/dgraph/push/rider-order", func(context *gin.Context) {
		var obj riderorder.DynamoStreamRecord
		context.BindJSON(&obj)
		fmt.Println("RiderOrder->>>>>>", obj)
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
