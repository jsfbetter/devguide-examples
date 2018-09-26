package main

import (
	"fmt"

	"github.com/couchbase/gocb"
	/* "encoding/json" */)

// bucket reference - reuse as bucket reference in the application
var bucket *gocb.Bucket

func main() {
	// Connect to Cluster
	cluster, err := gocb.Connect("couchbase://127.0.0.1")
	if err != nil {
		fmt.Println("ERROR CONNECTING TO CLUSTER:", err)
	}

	// Open Bucket
	bucket, err = cluster.OpenBucket("default", "123456")
	if err != nil {
		fmt.Println("ERROR OPENING BUCKET:", err)
	}

	// Create a view query
	view_query := gocb.NewViewQuery("default", "default")

	view_results, err := bucket.ExecuteViewQuery(view_query)
	if err != nil {
		fmt.Println("ERROR EXECUTE VIEW QUERY")
	}

	byte_result := view_results.NextBytes()
	fmt.Println(string(byte_result))

	var f map[string]interface{}
	ok := view_results.Next(&f)
	if !ok {
		fmt.Println("ERROR NEXT RESULT")
	} else {
		key := f["key"].(string)
		fmt.Println(key)
		/* var retValue interface{} */
		/* _, err = bucket.Get(key, &retValue) */
		/* if err != nil { */
		/* 	fmt.Println("ERROR RETURNING DOCUMENT:", err) */
		/* } */
		/* fmt.Println("Document Retrieved:", retValue) */
	}
}
