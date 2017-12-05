package referrals

/**
		ref_code: string @index(exact) .
        device_meta_data: string @index(exact) .
        GeneratedBy: uid @reverse @count .
        AppliedByUser: uid @reverse @count .
        AppliedByDevice: uid @reverse @count .
        FingerPrint:uid @count @reverse .
        user_id: int @index(int) .
*/

/**
		_:u1 <user_id> "123" .
        _:u2 <user_id> "456" .

        _:ref1 <ref_code> "SAT55" .
        _:dev1 <device_meta_data> "aef234" .
        _:dev2 <device_meta_data> "aef789" .

        _:ref1 <GeneratedBy> _:u1 .
        _:ref1 <AppliedByUser> _:u2 .
        _:ref1 <AppliedByDevice> _:dev2 .

        _:dev1 <FingerPrint> _:u1 .
        _:dev2 <FingerPrint> _:u2 .
*/
/*
func SearchAndInsert(ctx context.Context, code, phone, fingerPrintData string, userid int64, cl *client.Dgraph) error {

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
		Referrals    []dgraphdata.ReferralCode `json:"referral"`
		Advocates    []dgraphdata.UserDgraph   `json:"advocate"`
		Fingerprints []dgraphdata.Fingerprint  `json:"device_meta"`
		PhoneNumbers []dgraphdata.Phone        `json:"phone_number"`
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
*/
