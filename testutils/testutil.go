package testutils

import (
	"strings"
	"testing"

	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var (
	// TestCases are key-values to use in datastore testing
	TestCases = map[string]string{
		"/a":     "a",
		"/a/b":   "ab",
		"/a/b/c": "abc",
		"/a/b/d": "a/b/d",
		"/a/c":   "ac",
		"/a/d":   "ad",
		"/e":     "e",
		"/f":     "f",
		"/g":     "",
	}
)

// AddTestCases stores the key-value pairs in a datastore for testing
func AddTestCases(t *testing.T, d datastore.Datastore, testcases map[string]string) {
	t.Helper()
	for k, v := range testcases {
		dsk := datastore.NewKey(k)
		if err := d.Put(dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := datastore.NewKey(k)
		v2, err := d.Get(dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}
}

// ExpectMatches is used to match expected results with actual results
func ExpectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	t.Helper()
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

// ExpectKeyFilterMatches is used to verify filtering results
func ExpectKeyFilterMatches(t *testing.T, actual dsq.Results, expect []string) {
	t.Helper()
	actualE, err := actual.Rest()
	if err != nil {
		t.Error(err)
		return
	}
	actualS := make([]string, len(actualE))
	for i, e := range actualE {
		actualS[i] = e.Key
	}

	if len(actualS) != len(expect) {
		t.Error("length doesn't match.", expect, actualS)
		return
	}

	if strings.Join(actualS, "") != strings.Join(expect, "") {
		t.Error("expect != actual.", expect, actualS)
		return
	}
}

// ExpectKeyOrderMatches is used to verify ordering results
func ExpectKeyOrderMatches(t *testing.T, actual dsq.Results, expect []string) {
	t.Helper()
	rs, err := actual.Rest()
	if err != nil {
		t.Error("error fetching dsq.Results", expect, actual)
		return
	}

	if len(rs) != len(expect) {
		t.Error("expect != actual.", expect, actual)
		return
	}

	for i, r := range rs {
		if r.Key != expect[i] {
			t.Error("expect != actual.", expect, actual)
			return
		}
	}
}
