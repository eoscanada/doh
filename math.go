package main

import (
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
)

func msToBTTimestamp(in string) (bigtable.Timestamp, error) {
	if in == "" {
		return 0, nil
	}

	val, err := strconv.ParseInt(in, 10, 64)
	if err == nil {
		return bigtable.Timestamp(val) * 1000, nil
	}

	tm, err2 := time.Parse(time.RFC3339Nano, in)
	if err2 == nil {
		return bigtable.Time(tm), nil
	}

	return 0, fmt.Errorf("cannot parse as ms since EPOCH (%s), nor RFC3339-Nano (%s)", err, err2)
}
