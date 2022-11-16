package kvraft

import (
	"6.824/labgob"
	"bytes"
	log2 "log"
)

func (kv *KVServer) generatePersistData() []byte {
	//must hold lock before call
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.KVMap) != nil || e.Encode(kv.LastCommandID) != nil || e.Encode(kv.LastApplied) != nil {
		log2.Fatalf("Encode Error\n")
	}
	return w.Bytes()
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var lastCommandID map[int64]int
	var lastApplied int

	if d.Decode(&kvMap) != nil || d.Decode(&lastCommandID) != nil || d.Decode(&lastApplied) != nil {
		log2.Fatalf("Decode Error\n")
	} else {
		kv.KVMap = kvMap
		kv.LastCommandID = lastCommandID
		kv.LastApplied = lastApplied
	}
}
