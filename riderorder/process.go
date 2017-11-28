package riderorder

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"github.com/tokopedia/user-dgraph/dgraph"
	"log"
	"strconv"
	"strings"
)

type DynamoStreamRecord struct {
	Keys     map[string]map[string]string `json:"Keys"`
	OldImage Data                         `json:"OldImage"`
	NewImage Data                         `json:"NewImage"`
}

type Data struct {
	User_Id struct {
		Value string `json:"N"`
	} `json:"user_id"`
	Status struct {
		Value string `json:"S"`
	} `json:"status"`
	User_Email_Id struct {
		Value string `json:"S"`
	} `json:"guest_email"`
	User_First_Name struct {
		Value string `json:"S"`
	} `json:"guest_first_name"`
	User_Last_Name struct {
		Value string `json:"S"`
	} `json:"guest_last_name"`
	Driver_Name struct {
		Value string `json:"S"`
	} `json:"driver_name"`
	Driver_Phone_Number struct {
		Value string `json:"S"`
	} `json:"driver_phone_number"`
	User_Phone_Number struct {
		Value string `json:"S"`
	} `json:"guest_phone_number"`

	Device_Id struct {
		Value string `json:"S"`
	} `json:"device_id"`

	Device_Type struct {
		Value string `json:"S"`
	} `json:"device_type"`
	Vehicle_License_Plate struct {
		Value string `json:"S"`
	} `json:"vehicle_license_plate"`

	Pickup_Address struct {
		Value string `json:"S"`
	} `json:"pickup_address"`

	Destination_Address struct {
		Value string `json:"S"`
	} `json:"destination_address"`

	Destination_Longitude struct {
		Value string `json:"N"`
	} `json:"destination_longitude"`

	Destination_Latitude struct {
		Value string `json:"N"`
	} `json:"destination_latitude"`

	Pickup_Longitude struct {
		Value string `json:"N"`
	} `json:"pickup_longitude"`

	Pickup_Latitude struct {
		Value string `json:"N"`
	} `json:"pickup_latitude"`

	Request_Id struct {
		Value string `json:"S"`
	} `json:"request_id"`

	Ride_Amount struct {
		Value string `json:"N"`
	} `json:"total_amount"`
}

type PhoneNumber struct {
	UID   string `json:"uid,omitempty"`
	Name  string `json:"name,omitempty"`
	Phone string `json:"phone_number,omitempty"`
}
type Device struct {
	UID        string `json:"uid,omitempty"`
	Name       string `json:"name,omitempty"`
	DeviceId   string `json:"device_id,omitempty"`
	DeviceType string `json:"device_type,omitempty"`
}
type Vehicle struct {
	UID             string `json:"uid,omitempty"`
	Name            string `json:"name,omitempty"`
	VehicleNo       string `json:"vehicle_license_plate,omitempty`
	DriverName      string `json:driver_name,omitempty`
	PhoneNumberUsed []PhoneNumber
}
type Ride struct {
	UID            string `json:"uid,omitempty"`
	Name           string `json:"name,omitempty"`
	RideId         string `json:"ride_id,omitempty"`
	Rider          []User
	Driver         []Vehicle
	PickupLocation []Location
	DestLocation   []Location
}
type Location struct {
	UID            string `json:"uid,omitempty"`
	Name           string `json:"name,omitempty"`
	LocationCoords string `json:"location_coords,omitempty"`
}
type User struct {
	UID              string `json:"uid,omitempty"`
	Name             string `json:"name,omitempty"`
	Email            string `json:"user_email_id,omitempty"`
	UserId           string `json:"user_id,omitempty"`
	PhoneNumberUsed  []PhoneNumber
	DeviceOwned      []Device
	DrivenBy         []PhoneNumber
	PulsaPhoneNumber []PhoneNumber
}

type Root struct {
	User         []User        `json:"user"`
	UPhoneNumber []PhoneNumber `json:"uphone"`
	DPhoneNumber []PhoneNumber `json:"dphone"`
	Device       []Device      `json:"device"`
	Vehicle      []Vehicle     `json:"vehicle"`
	Ride         []Ride        `json:"ride"`
	PLocation    []Location    `json:"ploc"`
	DLocation    []Location    `json:"dloc"`
}

var ids = make(map[string]struct{})

func normalize(s string) string {
	dphone := strings.Replace(s, "-", "", -1)
	dphone = strings.Replace(dphone, "+", "", -1)
	return dphone
}

func normalizeAddress(s string) string {
	s = strings.Replace(s, `"`, "", -1)
	return strings.Join(strings.Fields(s), "")
}

func formatGeoLoc(f string) string {
	d, err := strconv.ParseFloat(f, 64)
	log.Println(f, err)
	s := fmt.Sprintf("%.3f", d)
	log.Println("s = ", s)

	return s
}

func concatLocations(lo string, la string) string {
	sa := make([]string, 2)

	sa[0] = formatGeoLoc(lo)
	sa[1] = formatGeoLoc(la)
	return strings.Join(sa, "^")
}

func concatName(lo string, la string) string {
	sa := make([]string, 2)

	sa[0] = lo
	sa[1] = la
	return strings.Join(sa, "_")
}

func LoadRideData(ctx context.Context, record *DynamoStreamRecord) {
	c := record.NewImage
	if c.Status.Value == "completed" {
		cl := dgraph.GetClient()
		writeToDgraph(ctx, cl, c)

	}
}

func writeToDgraph(ctx context.Context, ct *client.Dgraph, d Data) {
	txn := ct.NewTxn()
	defer txn.Discard(ctx)
	q := getQuery(d)
	log.Println(q)

	resp, err := txn.Query(ctx, q)
	if err != nil {
		log.Println(q, err)
		return
	}

	var r Root
	err = json.Unmarshal(resp.Json, &r)
	if err != nil {
		log.Println(err)
		return
	}

	err = upsertData(ctx, r, txn, d)
	if err != nil {
		log.Println(err)
		return
	}
}

func getQuery(c Data) string {

	const q = `
	{
		user(func: eq(user_id, "%v")) {
			uid
		}
		uphone(func: eq(phone_number, "%v")) {
			uid
		}
		dphone(func: eq(phone_number, "%v")) {
			uid
		}
		device(func: eq(device_id, "%v")) {
			uid
		}
		vehicle(func: eq(vehicle_license_plate, "%v")) {
			uid
		}
		ride(func: eq(ride_id, "%v")) {
			uid
		}
		ploc(func: eq(location_coords, "%v")) {
			uid
		}
		dloc(func: eq(location_coords, "%v")) {
			uid
		}
	}
`

	query := `{`

	if c.User_Id.Value != "" {
		query += fmt.Sprintf(`user(func: eq(user_id, "%v")) {
			uid
		}`, c.User_Id.Value)
	}

	phone := normalize(c.User_Phone_Number.Value)
	if phone != "" {
		query += fmt.Sprintf(`uphone(func: eq(phone_number, "%v")) {
			uid
		}`, phone)
	}

	phone = normalize(c.Driver_Phone_Number.Value)
	if phone != "" {
		query += fmt.Sprintf(`dphone(func: eq(phone_number, "%v")) {
			uid
		}`, phone)
	}

	if c.Device_Id.Value != "" {
		query += fmt.Sprintf(`device(func: eq(device_id, "%v")) {
			uid
		}`, c.Device_Id.Value)
	}

	if c.Vehicle_License_Plate.Value != "" {
		query += fmt.Sprintf(`vehicle(func: eq(vehicle_license_plate, "%v")) {
			uid
		}`, c.Vehicle_License_Plate.Value)
	}

	if c.Request_Id.Value != "" {
		query += fmt.Sprintf(`ride(func: eq(ride_id, "%v")) {
			uid
		}`, c.Request_Id.Value)
	}

	location := concatLocations(c.Pickup_Longitude.Value, c.Pickup_Latitude.Value)
	if location != "" {
		query += fmt.Sprintf(`ploc(func: eq(location_coords, "%v")) {
			uid
		}`, location)
	}

	location = concatLocations(c.Destination_Longitude.Value, c.Destination_Latitude.Value)
	if location != "" {
		query += fmt.Sprintf(`dloc(func: eq(location_coords, "%v")) {
			uid
		}`, location)
	}

	query += `}`

	return query

}

func upsertData(ctx context.Context, r Root, txn *client.Txn, c Data) error {
	q := `
	%v <user_id> %q .
	%v <device_id> %q .
	%v <vehicle_license_plate> %q .
	%v <location_coords> %q .
	%v <location_coords> %q .
	%v <ride_id> %q .
	%v <phone_number> %q .
	%v <phone_number> %q .

	%v <user_email_id> %q .
	%v <name> "DEVICE" .
	%v <name> "PHONE" .
	%v <name> "PHONE" .
	%v <user_name> %q .
	%v <DeviceOwned> %v .
	%v <PhoneNumberUsed> %v .

	%v <device_type> %q .
	%v <driver_name> %q .
	%v <name> "RIDE" .  	# label for ride
	%v <name> "LOCATION" .  	# label for location
	%v <name> "LOCATION" .  	# label for location

	%v <PickupLocation> %v .
	%v <DestinationLocation> %v .

	%v <ride_amount> %q .
	%v <pickup_address> %q .
	%v <destination_address> %q .
	%v <DrivenBy> %v .

	%v <Rider> %v .
	%v <Driver> %v .
	`
	var u, d, p, v, ri, pl, dl, dp string

	if len(r.User) == 0 {
		u = "_:u"
	} else {
		u = "<" + r.User[0].UID + ">"
	}

	if len(r.Device) == 0 {
		d = "_:d"
	} else {
		d = "<" + r.Device[0].UID + ">"
	}

	if len(r.UPhoneNumber) == 0 {
		p = "_:p"
	} else {
		p = "<" + r.UPhoneNumber[0].UID + ">"
	}

	if len(r.Vehicle) == 0 {
		v = "_:v"
	} else {
		v = "<" + r.Vehicle[0].UID + ">"
	}

	if len(r.Ride) == 0 {
		ri = "_:ri"
	} else {
		ri = "<" + r.Ride[0].UID + ">"
	}

	if len(r.PLocation) == 0 {
		pl = "_:pl"
	} else {
		pl = "<" + r.PLocation[0].UID + ">"
	}

	if len(r.DLocation) == 0 {
		dl = "_:dl"
	} else {
		dl = "<" + r.DLocation[0].UID + ">"
	}

	if len(r.DPhoneNumber) == 0 {
		dp = "_:dp"
	} else {
		dp = "<" + r.DPhoneNumber[0].UID + ">"
	}

	q = fmt.Sprintf(q,
		u, c.User_Id.Value,
		d, c.Device_Id.Value,
		v, c.Vehicle_License_Plate.Value,

		pl, concatLocations(c.Pickup_Longitude.Value, c.Pickup_Latitude.Value),
		dl, concatLocations(c.Destination_Longitude.Value, c.Destination_Latitude.Value),
		ri, c.Request_Id.Value,
		p, normalize(c.User_Phone_Number.Value),
		dp, normalize(c.Driver_Phone_Number.Value),
		u, c.User_Email_Id.Value,
		d, p, dp,
		u, concatName(c.User_First_Name.Value, c.User_Last_Name.Value),
		u, d,
		u, p,
		d, c.Device_Type.Value,
		v, c.Driver_Name.Value,
		ri, pl, dl,
		ri, pl,
		ri, dl,
		ri, c.Ride_Amount.Value,
		pl, normalizeAddress(c.Pickup_Address.Value),
		dl, normalizeAddress(c.Destination_Address.Value),
		u, dp,
		ri, u,
		ri, v)

	log.Println(q)

	mu := &protos.Mutation{SetNquads: []byte(q)}
	_, err := txn.Mutate(ctx, mu)

	if err != nil {
		return err
	}

	return txn.Commit(ctx)
}
