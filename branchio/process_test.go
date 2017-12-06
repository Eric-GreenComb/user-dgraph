package branchio

import (
	"context"
	"testing"
)

func TestLoadData(t *testing.T) {
	ctx := context.Background()
	req := []byte(`{"limited_ad_tracking_status":"0","metadata":{"key1":"value1","key2":"value2","key3":"value3","ip":"14.142.226.220"},"os":"Android","google_advertising_id":"11902479-1dc7-4205-804c-7bd2e8fcfe25","os_version":"26","event":"app share test","event_timestamp":"2017-11-29T09:37:27.822Z","hardware_id":"11902479-1dc7-4205-804c-7bd2e8fcfe25","ad_tracking_enabled":"true"}`)

	ProcessEvent(ctx, req)
}
