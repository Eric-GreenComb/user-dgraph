package branchio

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tokopedia/user-dgraph/utils"
	"github.com/tokopedia/wallet-oauth/common/log"
	"time"
)

const (
	ReferralGenerationEvent = "Referral Generated"
	ReferralUtilizingEvent  = "Referral Utilizing"
)

type EventData struct {
	Os                string   `json:"os"`
	GAD               string   `json:"google_advertising_id"`
	OSVersion         string   `json:"os_version"`
	Event             string   `json:"event"`
	EventTimestamp    string   `json:"event_timestamp"`
	HardwareId        string   `json:"hardware_id"`
	AdTrackingEnabled string   `json:"ad_tracking_enabled"`
	Metadata          Metadata `json:"metadata"`
}

type Metadata struct {
	Key1         string `json:"key1"`
	Key2         string `json:"key2"`
	Key3         string `json:"key3"`
	Ip           string `json:"ip"`
	ReferralCode string `json:"referral_code"`
	UserId       string `json:"user_id"`
}

//Sample {"limited_ad_tracking_status":"0","metadata":{"key1":"value1","key2":"value2","key3":"value3","ip":"14.142.226.220"},"os":"Android",
// "google_advertising_id":"11902479-1dc7-4205-804c-7bd2e8fcfe25","os_version":"26","event":"app share test",
// "event_timestamp":"2017-11-29T09:37:27.822Z","hardware_id":"11902479-1dc7-4205-804c-7bd2e8fcfe25","ad_tracking_enabled":"true"}
func LoadData(ctx context.Context, request []byte) {
	defer utils.PrintTimeElapsed(time.Now(), "Elapsed time LoadData:")

	eventData := EventData{}
	err := json.Unmarshal(request, &eventData)
	if err != nil {
		log.Println("Parsing error request:", err)
		return
	}
	eventName := eventData.Event
	if eventName == ReferralGenerationEvent {
		storeReferralAdvocate(ctx, eventData)
	} else if eventName == ReferralUtilizingEvent {

	}
}

func GenerateFingerprint(ctx context.Context, data EventData) (string, error) {
	//TODO: Some logic to generate fingerprint from eventdata, it should be idempotent and shouldn't be dependentent on the properties which changes frequently
	fingerPrint := fmt.Sprintf("%s%s%s%s%s", data.Os, data.OSVersion, data.GAD, data.HardwareId, data.Metadata.UserId)
	//TODO: Get some hash

	return fingerPrint, nil

}

func storeReferralAdvocate(ctx context.Context, data EventData) {

}
