package util

type Map map[string]interface{}

// ToBean ...
func (m Map) ToBean(v interface{}) error {
	val, err := Marshal(m)
	if err != nil {
		return err
	}
	return Unmarshal(val, v)
}

// Bean2Map ...
func Bean2Map(v interface{}) Map {
	val, _ := Marshal(v)
	m := make(Map)
	_ = Unmarshal(val, &m)
	return m
}

// CombinationBeans ... combine beans to a map
// e.g. CombinationBeans(bean1, bean2, bean3)
// return a map contains all fields of bean1, bean2, bean3
func CombinationBeans(beans ...interface{}) Map {
	m := make(Map)
	for _, bean := range beans {
		MergeMap(m, Bean2Map(bean))
	}
	return m
}

// MergeBean ...if source's field is not empty, then set target's field to source's field
// target and source must be pointer
// if source's field is empty, then target's field will not be changed
func MergeBean(target, source interface{}) error {
	t := Bean2Map(target)
	s := Bean2Map(source)
	MergeMap(t, s)
	return t.ToBean(target)
}

func MergeMap(target, source Map) {
	for k, v := range source {
		target[k] = v
	}
}

// StructToMap ...alias of Bean2Map
func StructToMap(v interface{}) Map { return Bean2Map(v) }
