package riderorder

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"github.com/heroku/go-getting-started/dgraph"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}


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

const query string = `{
  			u as var(func: eq(user_id, "%v")) @upsert
			up as var(func: eq(phone_number, "%v")) @upsert
			dp as var(func: eq(phone_number, "%v")) @upsert
			d as var(func: eq(device_id, "%v")) @upsert
			v as var(func: eq(vehicle_license_plate, "%v")) @upsert
			r as var(func: eq(ride_id, "%v")) @upsert
			pl as var(func: eq(location_coords, "%v")) @upsert
			dl as var(func: eq(location_coords, "%v")) @upsert
		}

		mutation {
		  set {
			uid(u) <user_email_id> "%v" .
			uid(u) <label> "%v" . 	# label for user
			uid(v) <label> "%v" . 	# label for vehicle
			uid(d) <name> "%v" . 	# label for device
			uid(dp) <name> "%v" . 	# label for phone
			uid(up) <name> "%v" . 	# label for phone
			uid(d) <device_type> "%v" .
			uid(v) <driver_name> "%v" .
			uid(r) <name> "%v" .  	# label for ride
			uid(pl) <name> "%v" .  	# label for location
			uid(dl) <name> "%v" .  	# label for location
			uid(u) <user_name> "%v" .

			uid(r) <pickup_location> uid(pl) .
			uid(r) <destination_location> uid(dl) .

			uid(r) <ride_amount> "%v" .
			uid(pl) <pickup_address> "%v" .
			uid(dl) <destination_address> "%v" .
			uid(pl) <pickup_pincode> "%v" .
			uid(dl) <destination_pincode> "%v" .

			uid(u) <device.owned> uid(d) .
			uid(u) <phone.number.used> uid(up) .
			uid(v) <phone.number.used> uid(dp) .
			uid(u) <driven.by> uid(dp) .

			uid(u) <rider> uid(r) .
			uid(v) <driver> uid(r) .

		  }
		}`

func getPincode(s string) string {
	arr := strings.Split(s, " ")
	if len(arr)-2 >= 0 {
		v := strings.Replace(arr[len(arr)-2], ",", "", -1)

		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return v
		} else {
			return "-999"
		}

	} else {
		return "-999"
	}
}

var ids = make(map[string]struct{})

func writeUserId(c Data) {
	_, ok := ids[c.User_Id.Value]

	if !ok {
		ids[c.User_Id.Value] = struct{}{} // add element
		fmt.Println("UserID:", c.User_Id.Value)
		//_, err:= fmt.Fprintf(w, "%v\n", c.User_Id.Value)
		//check(err)
	}

}

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
	check(err)
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

func LoadRideData(record *DynamoStreamRecord) {
	//file, err := os.Open("/Users/ajayk/Documents/rider_order/ride_order.json")
	//check(err)
	//defer file.Close()

	//scanOrder := bufio.NewScanner(file)

	//ul, err := os.Create("/Users/ajay/Documents/user_id_list.csv")
	//check(err)
	//defer ul.Close()

	//lw := bufio.NewWriter(ul)

	//scanOrder := record.NewImage

	//for scanOrder.Scan() {
	//	json.Unmarshal(scanOrder.Bytes(), record)
	//fmt.Println(record)
	//c, ok := record.NewImage.(RiderOrderData)
	//if !ok {
	//	fmt.Errorf("couldn't get the new Image:%v", ok)
	//	return
	//}

	c := record.NewImage
	if c.Status.Value == "completed" {
		writeUserId(c)

		q := fmt.Sprintf(query,
			c.User_Id.Value,
			normalize(c.User_Phone_Number.Value),
			normalize(c.Driver_Phone_Number.Value),
			c.Device_Id.Value,
			c.Vehicle_License_Plate.Value,
			c.Request_Id.Value,
			concatLocations(c.Pickup_Longitude.Value, c.Pickup_Latitude.Value),
			concatLocations(c.Destination_Longitude.Value, c.Destination_Latitude.Value),
			c.User_Email_Id.Value,
			"USER",
			"VEHICLE",
			"DEVICE", "PHONE", "PHONE", c.Device_Type.Value,
			c.Driver_Name.Value,
			"RIDE", "LOCATION", "LOCATION",
			concatName(c.User_First_Name.Value, c.User_Last_Name.Value),
			c.Ride_Amount.Value,
			normalizeAddress(c.Pickup_Address.Value),
			normalizeAddress(c.Destination_Address.Value),
			getPincode(c.Pickup_Address.Value),
			getPincode(c.Destination_Address.Value))

		dgraph.UpsertDgraph(q)

	}

	//}

	//lw.Flush()
}
