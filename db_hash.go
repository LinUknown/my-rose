package myRose

import "myRose/storage"

func (db *MyRose) HGet(key, field []byte) []byte {

	//db.mu.RLock()
	//defer db.mu.RUnlock()

	return db.hashIndex.HGet(string(key), string(field))
}

func (db *MyRose) HSet(key, field, value []byte) (res int, err error) {

	// todo: check value
	entry := storage.NewEntry(key, value, field, Hash, HashHSet)
	err = db.store(entry)
	if err != nil {
		return
	}

	res = db.hashIndex.HSet(string(key), string(field), value)
	return
}

func (db *MyRose) HDel(key, field []byte) (res int, err error) {
	entry := storage.NewEntry(key, nil, field, Hash, HashHDel)
	err = db.store(entry)
	if err != nil {
		return
	}

	res = db.hashIndex.HDel(string(key), string(field))
	return
}
