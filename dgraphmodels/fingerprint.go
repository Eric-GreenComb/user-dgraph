package dgraphmodels

type FingerprintDGraph struct {
	UID             string       `json:"uid,omitempty"`
	Name            string       `json:"name,omitempty"`
	FingerprintData string       `json:"fingerprint_data,omitempty"`
	Users           []UserDGraph `json:"~DeviceFingerPrint,omitempty"`
}
