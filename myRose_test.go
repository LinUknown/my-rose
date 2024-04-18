package myRose

import (
	"testing"
)

func TestMyRose_Close(t *testing.T) {

}

func TestOpen(t *testing.T) {
	db, err := Open()
	defer db.Close()
	if err != nil {
		t.Log(err)
		return
	}

	set, err := db.HSet([]byte("my_hash"), []byte("a"), []byte("hash_data_001"))
	if err != nil {
		t.Log(err)
		return
	}
	t.Log(set)

	result := db.HGet([]byte("my_hash"), []byte("a"))
	t.Log(string(result))
}

func TestOpenAgain(t *testing.T) {
	db, err := Open()
	defer db.Close()
	if err != nil {
		t.Log(err)
		return
	}

	result := db.HGet([]byte("my_hash"), []byte("a"))
	t.Log(string(result))
}
