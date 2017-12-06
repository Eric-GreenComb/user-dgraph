package dgraphmodels

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/tokopedia/user-dgraph/dgraph"
	"log"
)

/**
		ref_code: string @index(exact) .
        device_meta_data: string @index(exact) .
        GeneratedBy: uid @reverse @count .
        AppliedByUser: uid @reverse @count .
        AppliedByDevice: uid @reverse @count .
        FingerPrint:uid @count @reverse .
        user_id: int @index(int) .
*/

type ReferralDGraph struct {
	UID             string              `json:"uid,omitempty"`
	Name            string              `json:"name,omitempty"`
	RefCode         string              `json:"ref_code,omitempty"`
	GeneratedBy     []PhoneDGraph       `json:"GeneratedBy,omitempty"`
	AppliedByDevice []FingerprintDGraph `json:"AppliedByDevice,omitempty"`
}

func GetAdvocateDetails(ctx context.Context, c *client.Dgraph, code string) (ReferralDGraph, error) {
	txn := c.NewTxn()
	defer txn.Discard(ctx)

	q := fmt.Sprintf(`{
			get_referral_advocate(func: eq(ref_code, "%s")) @cascade{
				uid
				ref_code
				GeneratedBy{
					uid
					user_id
				}
			}
		}`, code)

	resp, err := txn.Query(ctx, q)
	if err != nil {
		log.Println(q, err)
		return ReferralDGraph{}, err
	}

	var referralCodeDecode struct {
		ReferralCode ReferralDGraph `json:"get_referral_advocate"`
	}

	if err := json.Unmarshal(resp.GetJson(), &referralCodeDecode); err != nil {
		log.Println(resp, err)
		return ReferralDGraph{}, err
	}

	return referralCodeDecode.ReferralCode, nil
}

func SearchAndInsertReferral(ctx context.Context, code, phone, fingerPrintData string, userid int64, cl *client.Dgraph) error {

	q := `
	{
		referral(func: eq(ref_code, %q)) {
			uid

		}
		advocate(func: eq(user_id, %q)) {
			uid
		}
		device_meta(func: eq(fingerprint_data, %q)){
			uid
		}
		phone_number(func: eq(phone_number, %q)){
			uid
		}
	}`

	x := fmt.Sprintf(q, code, userid, fingerPrintData, phone)

	txn := cl.NewTxn()
	resp, err := txn.Query(ctx, x)

	if err != nil {
		log.Println(fmt.Sprintf("Couldn't fetch referral(%s)/advocate(%s) from DGraph with error:%v", code, userid, err))
		return err
	}

	var decodeObj struct {
		Referrals    []ReferralDGraph    `json:"referral"`
		Advocates    []UserDGraph        `json:"advocate"`
		Fingerprints []FingerprintDGraph `json:"device_meta"`
		PhoneNumbers []PhoneDGraph       `json:"phone_number"`
	}

	err = json.Unmarshal(resp.Json, &decodeObj)
	if err != nil {
		log.Println("Unmarshal error:", err)
		return err
	}

	if len(decodeObj.Referrals) != 0 {
		return nil
	}

	//Prepare query for mutation
	var ref, ph, u, fp string
	ref = "_:ref"
	q = fmt.Sprintf(`
	%v <ref_code> %q .
	`, ref, code)

	if len(decodeObj.PhoneNumbers) == 0 {
		ph = "_:ph"
		q += fmt.Sprintf(`%v <phone_number> %q .
				%v <name> "PHONE" .
			`, ph, phone, ph)
	} else {
		ph = fmt.Sprintf("<%s>", decodeObj.PhoneNumbers[0].UID)
	}

	q += fmt.Sprintf(`%v <GeneratedBy> %v .
			`, ref, ph)

	if len(decodeObj.Advocates) == 0 {
		u = "_:u"
		q += fmt.Sprintf(`%v <user_id> %q .
			%v <name> "USER" .
			`, u, userid, u)
	} else {
		u = fmt.Sprintf("<%s>", decodeObj.Advocates[0].Uid)
	}

	q += fmt.Sprintf(`%v <PhoneNumberUsed> %v .
			`, u, ph)

	if len(decodeObj.Fingerprints) == 0 {
		fp = "_:fp"
		q += fmt.Sprintf(`%v <fingerprint_data> %q .
				%v <name> "FINGERPRINT" .
				`, fp, fingerPrintData, fp)
	} else {
		fp = fmt.Sprintf("<%s>", decodeObj.Fingerprints[0].UID)
	}

	q += fmt.Sprintf(`%v <DeviceFingerPrint> %v .`, u, fp)

	log.Println(q)

	err = dgraph.RetryMutate(ctx, cl, q, dgraph.DGraphMutationRetryCount)
	if err != nil {
		log.Println(q, err)
	} else {
		log.Println("Successfully pushed to dgraph.")
	}
	return err
}

func GetExistingReferral(ctx context.Context, code string, cl *client.Dgraph) (referralUid string, fraudUsers []UserDGraph, err error) {
	q := fmt.Sprintf(`{
		referral(func: eq(ref_code, %q)) {
			uid
			ref_code
			AppliedByDevice{
				uid
				~DeviceFingerPrint{
					uid
					user_id
				}
			}
		}
		}`, code)

	txn := cl.NewTxn()
	resp, err := txn.Query(ctx, q)

	if err != nil {
		log.Println(fmt.Sprintf("Couldn't fetch referral(%s) from DGraph with error:%v", code, err))
		return "", nil, err
	}

	var decodeObj struct {
		Referrals []ReferralDGraph `json:"referral"`
	}
	//TODO: Define a datatype to get the users for DGraph
	err = json.Unmarshal(resp.Json, &decodeObj)
	if err != nil {
		log.Println("Unmarshal error:", err)
		return "", nil, err
	}

	if len(decodeObj.Referrals) == 0 {
		return "", []UserDGraph{}, nil
	}

	if len(decodeObj.Referrals[0].AppliedByDevice) == 0 {
		return decodeObj.Referrals[0].UID, []UserDGraph{}, nil
	}

	//var userDgraphModels []UserDGraph

	for _, d := range decodeObj.Referrals[0].AppliedByDevice {
		fraudUsers = append(fraudUsers, d.Users...)
	}

	return decodeObj.Referrals[0].UID, fraudUsers, nil
}

func InsertAppliedReferral() {

}

//
/*func SaveReferralCode(ctx context.Context, userid int64, code string, c *client.Dgraph) (string, error) {
	//First get the user's dGraph uid
	users, err := users.GetUsersUIDs(fmt.Sprintf(`"%s"`, userid), c)
	if err != nil {
		log.Println(fmt.Sprintf("Couldn't get the user(%v) from dgraph with error:%v", userid, err))
		return "", err
	}

	if len(users) > 1 {
		msg := fmt.Sprintf("Error:Multiple users associated with userid:", userid)
		return "", errors.New(msg)
	}

	userUid := ""
	if len(users) == 0 {
		userUid, err = users.CreateUser(userid, c)

		if err != nil {
			log.Println(fmt.Sprintf("Couldn't create the user(%v) in dgraph with error:%v", userid, err))
			return "", nil
		}
	} else {
		userUid = users[0].Uid
	}

	//Create Json for referral code
	referralCode := ReferralCode{
		RefCode:     code,
		Name:        "ReferralCode",
		GeneratedBy: users.UserDgraph{Uid: userUid},
	}

	//Store the Json
	mu := &protos.Mutation{CommitNow: true}
	refCodeJson, err := json.Marshal(referralCode)
	if err != nil {
		log.Println("Marshal error:", referralCode, err)
		return "", err
	}

	mu.SetJson = refCodeJson
	assigned, err := c.NewTxn().Mutate(ctx, mu)
	if err != nil {
		log.Println("Dgraph ReferralCode saving error", mu, err)
		return "", err
	}
	return assigned.Uids["blank-0"], nil
}*/
