package database

import (
	"database/sql"
	"fmt"
	"log"
)

var (
	ConnUser = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		"192.168.17.29", "az170907", "readorder@321#", "tokopedia-user")
	UserDbCon *sql.DB
)

func init() {
	var err error
	UserDbCon, err = sql.Open("postgres", ConnUser)
	if err != nil {
		log.Fatal("Couldn't connect to postgres:", err)
	}
	UserDbCon.SetMaxOpenConns(80)
	log.Println("Got postgres user conn object")
}
