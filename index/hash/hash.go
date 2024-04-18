package hash

type Record map[string]map[string][]byte

// 定义Hash对象结构体，每个h对象，都有一个record属性，类型为Record
type Hash struct {
	record Record
}

func New() *Hash {
	return &Hash{
		record: make(Record),
	}
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}

func (h *Hash) HSet(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	h.record[key][field] = value
	return len(h.record[key])
}

func (h *Hash) HDel(key string, field string) int {
	if !h.exist(key) {
		return 0
	}

	delete(h.record[key], field)
	return len(h.record[key])
}

func (h *Hash) HGet(key string, field string) (value []byte) {
	if !h.exist(key) {
		return nil
	}
	return h.record[key][field]
}
