package fingerprint

import "github.com/tokopedia/user-dgraph/dgraph/users"

type DGraphModel struct {
	UID             string              `json:"uid,omitempty"`
	Name            string              `json:"name,omitempty"`
	FingerprintData string              `json:"fingerprint_data,omitempty"`
	Users           []users.DGraphModel `json:"~DeviceFingerPrint,omitempty"`
}
