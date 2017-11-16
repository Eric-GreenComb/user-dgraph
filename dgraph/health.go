package dgraph

import "strings"

func CheckHealth() string{

	query := `{
  				q(func:eq(user_id,"992061")){
    				user_id
  				}
			}`
	message := UpsertDgraph(query)

	if strings.Contains(message, "992061") {
		return "OK"
	}else{
		return message
	}
}
