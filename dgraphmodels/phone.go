package dgraphmodels

type PhoneDGraph struct {
	UID         string `json:"uid,omitempty"`
	Name        string `json:"name,omitempty"`
	PhoneNumber string `json:"phone_number"`
}
