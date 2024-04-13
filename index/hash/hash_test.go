package hash

import "testing"

// test hash

func TestHash_HSetAndHGet(t *testing.T) {
	hash := &Hash{
		record: make(Record),
	}
	n := hash.HSet("my_hash", "a", []byte("hash_data_001"))
	t.Log(n)
	value := hash.HGet("my_hash", "a")
	t.Log(string(value))
}
