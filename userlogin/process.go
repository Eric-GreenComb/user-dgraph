package userlogin

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/dgraph-io/dgraph/client"
	_ "github.com/lib/pq"
	"github.com/tokopedia/user-dgraph/database"
	"github.com/tokopedia/user-dgraph/dgraph"
	"github.com/tokopedia/user-dgraph/riderorder"
	"github.com/tokopedia/user-dgraph/utils"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Result struct {
	User        []riderorder.User        `json:"user"`
	Fingerprint []Fingerprint            `json:"fingerprint"`
	PhoneNumber []riderorder.PhoneNumber `json:"phone"`
}

type Fingerprint struct {
	UID              string `json:"uid,omitempty"`
	Name             string `json:"name,omitempty"`
	Fingerprint_Data string `json:"fingerprint_data,omitempty"`
}

type userdata struct {
	user_email_id sql.NullString
	user_name     sql.NullString
}

/*
var (
	connUser = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		"192.168.17.29", "az170907", "readorder@321#", "tokopedia-user")
	//userids = make(map[string]userdata)
)
*/

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

func LoadUserLoginData(ctx context.Context, request []byte) {
	defer utils.PrintTimeElapsed(time.Now(), "Elapsed time for LoadUserLoginData:")
	/*db, err := sql.Open("postgres", connUser)

	if err != nil {
		log.Fatal("Couldn't connect to postgres:", err)
	}
	defer db.Close()*/

	uid, err := jsonparser.GetString(request, "NewImage", "uid", "S")
	if err != nil {
		log.Println("Doesn't contains the uid, exiting")
		return
	}

	uids := ""
	if uid != "" {
		uids = fmt.Sprintf("%s", uid)

		_, ez := strconv.Atoi(uids)

		if ez != nil {
			log.Println("Uid is not numeric:", ez)
			return
		}
	}

	shaHash := getFingerprintHash(request, uids)
	nos, err := getPhoneNos(request)
	if err != nil {
		log.Println("Error in getting phonenos from image:", err)
		return
	}

	log.Printf("before uploading in graph, nos: %v, fin: %v\n", len(nos), len(shaHash))

	if len(nos) > 0 || len(shaHash) > 0 {
		c := dgraph.GetClient()
		udata, err := getUserDetails(uids)
		if err != nil {
			log.Println("Postgres error for uid:", uids, err)
			return
		}
		go writetoDgraph(ctx, c, uids, udata, shaHash, nos)
		//writetoDgraph(uids, userdata{}, shaHash, nos)
	}
}

func getFingerprintHash(js []byte, uids string) []string {
	newShaHash := make([]string, 0)
	oldShaHash := make([]string, 0)

	jsonparser.ArrayEach(js, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {

		finger, _, _, e3 := jsonparser.Get(value, "S")

		if e3 == nil {
			s, _ := base64.StdEncoding.DecodeString(fmt.Sprintf("%s", finger))

			if strings.Contains(string(s[:]), "android") ||
				strings.Contains(string(s[:]), "iPhone") {
				hasher := sha1.New()
				hasher.Write(finger)
				newShaHash = append(newShaHash, hex.EncodeToString(hasher.Sum(nil)))
			}
		}

	}, "NewImage", "user_data", "M", "filtron", "M", "uuid_"+uids, "M", "fingerprint_data", "L")

	jsonparser.ArrayEach(js, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {

		finger, _, _, e3 := jsonparser.Get(value, "S")

		if e3 == nil {
			s, _ := base64.StdEncoding.DecodeString(fmt.Sprintf("%s", finger))

			if strings.Contains(string(s[:]), "android") ||
				strings.Contains(string(s[:]), "iPhone") {
				hasher := sha1.New()
				hasher.Write(finger)
				oldShaHash = append(oldShaHash, hex.EncodeToString(hasher.Sum(nil)))
			}
		}

	}, "OldImage", "user_data", "M", "filtron", "M", "uuid_"+uids, "M", "fingerprint_data", "L")

	//log.Println("OldAndNewImageFingerprint:", oldShaHash, newShaHash)

	if len(oldShaHash) != len(newShaHash) {
		return newShaHash
	}
	sort.Strings(oldShaHash)
	sort.Strings(newShaHash)

	allEqual := true

	for i, _ := range newShaHash {
		if newShaHash[i] != oldShaHash[i] {
			allEqual = false
			break
		}
	}

	if allEqual {
		return make([]string, 0)
	}
	log.Println("Going to push fingerprint:", oldShaHash, newShaHash)
	return newShaHash
}

func getPhoneNos(js []byte) ([]string, error) {

	newPhoneNos := make([]string, 0)
	oldPhoneNos := make([]string, 0)
	category := [4]string{"1", "2", "9", "20"}

	for _, v := range category {
		client, _, _, e4 := jsonparser.Get(js, "NewImage", "user_data", "M", "digital", "M", "category_"+v, "M")
		if e4 == nil {
			clientno, err := getClientNumber(client)

			if err != nil {
				return newPhoneNos, err
			}
			if len(strings.TrimSpace(clientno)) > 0 {
				newPhoneNos = append(newPhoneNos, clientno)
			}

		}
	}

	for _, v := range category {
		client, _, _, e4 := jsonparser.Get(js, "OldImage", "user_data", "M", "digital", "M", "category_"+v, "M")
		if e4 == nil {
			clientno, err := getClientNumber(client)

			if err != nil {
				return newPhoneNos, err //Old Image is having issues so return newImage's phoneNos
			}
			if len(strings.TrimSpace(clientno)) > 0 {
				oldPhoneNos = append(oldPhoneNos, clientno)
			}

		}
	}

	//log.Println("OldAndNewImagePhone:", oldPhoneNos, newPhoneNos)
	if len(oldPhoneNos) != len(newPhoneNos) {
		return newPhoneNos, nil
	}

	sort.Strings(oldPhoneNos)
	sort.Strings(newPhoneNos)

	allEquals := true
	for i, _ := range newPhoneNos {
		if newPhoneNos[i] != oldPhoneNos[i] {
			allEquals = false
			break
		}
	}

	if allEquals {
		return make([]string, 0), nil
	}

	log.Println("Goint to push phone:", oldPhoneNos, newPhoneNos)
	return newPhoneNos, nil
}

func writetoDgraph(ctx context.Context, ct *client.Dgraph, userid string, usr userdata, finger []string, phones []string) {
	defer utils.PrintTimeElapsed(time.Now(), "Elapsed time for LoadUserLoginData-writetoDgraph:")

	q1 := `
	{
		user(func: eq(user_id, %q)) {
			uid
		}
		fingerprint(func:eq(fingerprint_data,%q)){
			uid
		}
	}`

	q2 := `
	{
		user(func: eq(user_id, %q)) {
			uid
		}
		phone(func:eq(phone_number,%q)){
			uid
		}
	}`

	for _, v := range finger {
		q := fmt.Sprintf(q1, userid, v)
		r := Result{}
		r, err := searchDGraph(ctx, ct, q)
		if err != nil {
			log.Println(q, err)
			return
		}
		err = upsertFingerprint(ctx, ct, r, usr, userid, v)
		if err != nil {
			return
		}
	}

	for _, va := range phones {
		q := fmt.Sprintf(q2, userid, va)
		r, err := searchDGraph(ctx, ct, q)
		if err != nil {
			log.Println(q, err)
			return
		}
		err = upsertPhone(ctx, ct, r, usr, userid, va)
		if err != nil {
			return
		}
	}
}

func upsertPhone(ctx context.Context, cl *client.Dgraph, r Result, usr userdata, userid string, p string) error {

	q := `
		%v <user_id> %q .
		%v <phone_number> %q .
		%v <name> "PHONE" .
		%v <name> "USER" .
		%v <user_name> %q .
		%v <user_email_id> %q .
		%v <PulsaPhoneNumber> %v .`

	var f, u string

	if len(r.User) == 0 {
		u = "_:u"
	} else {
		u = "<" + r.User[0].UID + ">"
	}

	if len(r.PhoneNumber) == 0 {
		f = "_:f"
	} else {
		f = "<" + r.PhoneNumber[0].UID + ">"
	}

	q = fmt.Sprintf(q,
		u, userid,
		f, p,
		f, u,
		u, getValidValues(usr.user_name),
		u, getValidValues(usr.user_email_id),
		u, f)

	err := dgraph.RetryMutate(ctx, cl, q, dgraph.DGraphMutationRetryCount)
	if err != nil {
		log.Println(q, err)
	} else {
		log.Println("Successfully pushed to dgraph.")
	}
	return err
}

func upsertFingerprint(ctx context.Context, cl *client.Dgraph, r Result, usr userdata, userid string, fp string) error {

	q := `
	%v <user_id> %q .
	%v <fingerprint_data> %q .
	%v <name> "FINGERPRINT" .
	%v <name> "USER" .
	%v <user_name> %q .
	%v <user_email_id> %q .
	%v <DeviceFingerPrint> %v .`

	var f, u string

	if len(r.User) == 0 {
		u = "_:u"
	} else {
		u = "<" + r.User[0].UID + ">"
	}

	if len(r.Fingerprint) == 0 {
		f = "_:f"
	} else {
		f = "<" + r.Fingerprint[0].UID + ">"
	}

	q = fmt.Sprintf(q,
		u, userid,
		f, fp,
		f, u,
		u, getValidValues(usr.user_name),
		u, getValidValues(usr.user_email_id),
		u, f)

	err := dgraph.RetryMutate(ctx, cl, q, dgraph.DGraphMutationRetryCount)
	if err != nil {
		log.Println(q, err)
	} else {
		log.Println("Successfully pushed to dgraph.")
	}
	return err
}

func searchDGraph(ctx context.Context, cl *client.Dgraph, q string) (Result, error) {
	txn := cl.NewTxn()
	defer txn.Discard(ctx)

	var r Result
	resp, err := txn.Query(ctx, q)
	if err != nil {
		return r, err
	}

	err = json.Unmarshal(resp.Json, &r)
	if err != nil {
		return r, err
	}
	return r, nil
}

func getValidValues(s sql.NullString) string {
	if s.Valid {
		return s.String
	} else {
		return "NONE"
	}
}

func getClientNumber(arr []byte) (string, error) {
	var j map[string]interface{}
	err := json.Unmarshal([]byte(arr), &j)
	if err != nil {
		return "", err
	}

	for _, value := range j {

		for _, val := range value.(map[string]interface{}) {

			for _, val2 := range val.(map[string]interface{}) {

				for _, val3 := range val2.(map[string]interface{}) {

					for key1, val4 := range val3.(map[string]interface{}) {

						if key1 == "client_number" {
							for _, val5 := range val4.(map[string]interface{}) {
								return val5.(string), nil

							}
						}
					}
				}
			}
		}

	}

	return "", nil
}

func getUserDetails(uid string) (userdata, error) {

	/*c, ok := userids[uid]

	if ok {
		return c, nil
	}*/
	db := database.UserDbCon
	if db == nil {
		log.Fatal("Don't have db connection yet.")
	}
	err := db.Ping()

	if err != nil {
		log.Println("Error: Could not establish a connection with the database")
		return userdata{}, err
	}

	query := `select user_name,user_email from ws_user
				where user_id =$1`

	var bd userdata

	err = db.QueryRow(query, uid).Scan(&bd.user_name, &bd.user_email_id)
	if err != nil {
		log.Fatal(err)
	}

	/*rows, err := db.Query(query, uid)
	if err != nil {
		return userdata{}, err
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&bd.user_name, &bd.user_email_id); err != nil {
			return userdata{}, err
		}
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}*/

	log.Println("UserFromDB = ", bd)
	//userids[uid] = bd
	return bd, nil
}
