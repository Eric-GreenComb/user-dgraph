package userlogin

import (
	"bytes"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/buger/jsonparser"
	"log"
	"strconv"
	"strings"
	"time"
	"github.com/tokopedia/user-dgraph/dgraph"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type userdata struct {
	user_email_id sql.NullString
	user_name     sql.NullString
}

var (
	connUser = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		"192.168.17.29", "az170907", "readorder@321#", "tokopedia-user")
	userids = make(map[string]userdata)
)

type DynamoStreamRecord struct {
	Keys     map[string]map[string]string `json:"Keys"`
	OldImage Data                         `json:"OldImage"`
	NewImage Data                         `json:"NewImage"`
}

type Data struct {
	User_Id struct {
		Value string `json:"S"`
	} `json:"uid"`

	UserData struct {
		Value json.RawMessage `json:"M"`
	} `json:"user_data"`
}

func GetBytes(key interface{}) ([]byte, error) {
	gob.Register(map[string]interface{}{})
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func LoadUserLoginData(request []byte) {

	/*db, err := sql.Open("postgres", connUser)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()*/

	uid, err := jsonparser.GetString(request, "NewImage", "uid", "S")
	if err != nil {
		fmt.Println("Doesn't contains the uid, exiting")
		return
	}

	uids := ""
	if uid != "" {
		uids = fmt.Sprintf("%s", uid)

		_, ez := strconv.Atoi(uids)

		if ez != nil {
			fmt.Println("Got error:", ez)
			return
		}
	}
	log.Println("Uid =" + uids)
	fmt.Printf("started processing record for uid %v::%v\n", uids, time.Now())
	//js, err := GetBytes(record.NewImage.UserData.Value)
	//
	//if err != nil {
	//	fmt.Println("couldn't get []byte from record:", err)
	//	return
	//}

	shaHash := getFingerprintHash(request, uids)

	nos := getPhoneNos(request)

	log.Printf("before uploading in graph, nos: %v, fin: %v\n", len(nos), len(shaHash))

	if len(nos) > 0 || len(shaHash) > 0 {
		//writetoDgraph(uids, getUserDetails(uids, db), shaHash, nos)
		writetoDgraph(uids, userdata{}, shaHash, nos)
	}

	fmt.Printf("completed processing record for uid %v::%v\n", uids, time.Now())

}

func getFingerprintHash(js []byte, uids string) []string {
	shaHash := make([]string, 0)

	jsonparser.ArrayEach(js, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {

		finger, _, _, e3 := jsonparser.Get(value, "S")

		if e3 == nil {
			s, _ := base64.StdEncoding.DecodeString(fmt.Sprintf("%s", finger))

			if strings.Contains(string(s[:]), "android") ||
				strings.Contains(string(s[:]), "iPhone") {
				hasher := sha1.New()
				hasher.Write(finger)
				shaHash = append(shaHash, hex.EncodeToString(hasher.Sum(nil)))
			}
		}
		fmt.Println("Got Finger::", string(finger))

	}, "NewImage", "user_data", "M", "filtron", "M", "uuid_"+uids, "M", "fingerprint_data", "L")

	return shaHash
}

func getPhoneNos(js []byte) []string {

	phonenos := make([]string, 0)
	category := [4]string{"1", "2", "9", "20"}

	for _, v := range category {
		client, _, _, e4 := jsonparser.Get(js, "NewImage", "user_data", "M", "digital", "M", "category_"+v, "M")
		if e4 == nil {
			clientno := getClientNumber(client)

			if len(strings.TrimSpace(clientno)) > 0 {
				phonenos = append(phonenos, clientno)
			}

		}
	}
	log.Println("PhoneNos:", phonenos)
	return phonenos
}

func writetoDgraph(userid string, usr userdata, finger []string, phones []string) {

	query :=
		`{
  			u as var(func: eq(user_id, "%v")) @upsert
			f as var(func:eq(fingerprint_data,"%v")) @upsert

		}

		mutation {
		  set {
			uid(f) <name> "FINGERPRINT" .
			uid(u) <name> "USER" .
			uid(u) <user_name> "%v" .
			uid(u) <user_email_id> "%v" .
			uid(u) <device.finger.print> uid(f) .
		  }
		}`

	query2 :=
		`{
  			u as var(func: eq(user_id, "%v")) @upsert
			p as var(func: eq(phone_number, "%v")) @upsert
		}

		mutation {
		  set {
			uid(u) <pulsa.phone.number> uid(p) .
			uid(u) <name> "USER" .
			uid(p) <name> "PHONE" .
			uid(u) <user_name> "%v" .
			uid(u) <user_email_id> "%v" .
		  }
		}`

	//fmt.Println("UserLogin FingerPrint Query1:", query)
	//fmt.Println("UserLogin FingerPrint Query2:", query2)
	for _, v := range finger {
		dgraph.UpsertDgraph(fmt.Sprintf(query, userid, v, getValidValues(usr.user_name), getValidValues(usr.user_email_id)))
	}

	for _, va := range phones {
		dgraph.UpsertDgraph(fmt.Sprintf(query2, userid, va, getValidValues(usr.user_name), getValidValues(usr.user_email_id)))

	}
}

func getValidValues(s sql.NullString) string {
	if s.Valid {
		return s.String
	} else {
		return "NONE"
	}
}

func getClientNumber(arr []byte) string {
	var j map[string]interface{}
	err := json.Unmarshal([]byte(arr), &j)
	if err != nil {
		check(err)
	}

	for _, value := range j {

		for _, val := range value.(map[string]interface{}) {

			for _, val2 := range val.(map[string]interface{}) {

				for _, val3 := range val2.(map[string]interface{}) {

					for key1, val4 := range val3.(map[string]interface{}) {

						if key1 == "client_number" {
							for _, val5 := range val4.(map[string]interface{}) {
								return val5.(string)

							}
						}
					}
				}
			}
		}

	}
	return ""
}

func getUserDetails(uid string, db *sql.DB) userdata {

	c, ok := userids[uid]

	log.Println("ok =", ok)

	if ok {
		return c
	}

	err := db.Ping()

	if err != nil {
		log.Fatal("Error: Could not establish a connection with the database")
		check(err)
	}

	log.Println("CONN SUCCESS =")
	query := `select user_name,user_email from ws_user
				where user_id =$1`

	var bd userdata

	rows, err := db.Query(query, uid)
	check(err)
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&bd.user_name, &bd.user_email_id); err != nil {
			check(err)
		}
	}

	log.Println("Bd = ", bd)
	userids[uid] = bd
	return bd
}
