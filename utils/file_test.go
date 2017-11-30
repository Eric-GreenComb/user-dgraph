package utils

import (
	"github.com/prometheus/common/log"
	"testing"
)

func TestWriteDiff(t *testing.T) {
	dir := "/Users/ajayk/Documents/dgraph/dropshippers/analysis"
	src := dir + "/dgraph_phone_match_got_cashback.txt"
	toComp := dir + "/soron_all_srns.txt"
	dest := dir + "/soron_couldnt_find.txt"

	err := WriteDiff(src, toComp, dest)
	if err != nil {
		log.Fatal(err)
	}

}
