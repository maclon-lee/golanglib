package utility

import (
	"fmt"
	"hash/crc32"
	"reflect"
	"time"
)

//计算hashcode，目前只支持int，string等基础类型
func GetHashcode(v interface{}) int32 {
	switch t := v.(type) {
	case int:
		return int32(t)
	case int8:
		return int32(t)
	case int16:
		return int32(t)
	case int32:
		return t
	case int64:
		return int32(t)
	case uint:
		return int32(t)
	case uint8:
		return int32(t)
	case uint16:
		return int32(t)
	case uint32:
		return int32(t)
	case uint64:
		return int32(t)
	case time.Time:
		return int32(t.Unix())
	case reflect.Value:
		if t.IsValid() && t.CanInterface() {
			return GetHashcode(t.Interface())
		}
	}
	return GetStringCode(fmt.Sprintf("%v", v))
}

func GetStringCode(str string) int32 {
	v := int32(crc32.ChecksumIEEE([]byte(str)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	return 0
}
