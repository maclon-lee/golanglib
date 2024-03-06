package utility

import (
	"errors"
	"gopkg.in/guregu/null.v3"
	"os"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

const (
	//日期时间格式
	FORMATDATETIME = "2006-01-02 15:04:05"

	//日期格式
	FORMATDATE = "2006-01-02"

	//数据库时间格式
	FORMATSQLTIME = "2006-01-02 15:04:05.999999999Z07:00"
	FORMATMSSQLTIME = "2006-01-02T15:04:05Z07:00"

	//日志时间格式
	FORMATLOGTIME = "1月2日 15:04:05"
)

var TIMEZONE *time.Location

func init() {
	//设置默认时区
	TIMEZONE, _ = time.LoadLocation("Asia/Shanghai")
}

// StringToBytes converts string to byte slice without a memory allocation.
func StringToBytes(s string) (b []byte) {
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data, bh.Len, bh.Cap = sh.Data, sh.Len, sh.Len
	return b
}

// BytesToString converts byte slice to string without a memory allocation.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

//Map数据转换为Struct结构（原始变量类型）
func TransfMap2Struct(source map[string]interface{}, inStructPtr interface{}) error {
	rType := reflect.TypeOf(inStructPtr)
	rVal := reflect.ValueOf(inStructPtr)
	if rType.Kind() == reflect.Ptr {
		// 传入的inStructPtr是指针，需要.Elem()取得指针指向的value
		rType = rType.Elem()
		rVal = rVal.Elem()
	} else {
		return errors.New("inStructPtr must be ptr to struct")
	}

	// 遍历结构体
	for i := 0; i < rType.NumField(); i++ {
		t := rType.Field(i)
		f := rVal.Field(i)

		if v, ok := source[t.Name]; ok && v != nil {
			f.Set(reflect.ValueOf(v))
		}
	}
	return nil
}

//Map数据转换为Struct结构（null变量类型，包：gopkg.in/guregu/null.v3）
func TransfMap2StructNulltype(source map[string]interface{}, inStructPtr interface{}) error {
	rType := reflect.TypeOf(inStructPtr)
	rVal := reflect.ValueOf(inStructPtr)
	if rType.Kind() == reflect.Ptr {
		// 传入的inStructPtr是指针，需要.Elem()取得指针指向的value
		rType = rType.Elem()
		rVal = rVal.Elem()
	} else {
		return errors.New("inStructPtr must be ptr to struct")
	}

	// 遍历结构体
	for i := 0; i < rType.NumField(); i++ {
		t := rType.Field(i)
		f := rVal.Field(i)

		if v, ok := source[t.Name]; ok && v != nil {
			switch v.(type) {
			case string:
				f.Set(reflect.ValueOf(null.StringFrom(v.(string))))
			case int32:
				f.Set(reflect.ValueOf(null.IntFrom(int64(v.(int32)))))
			case int64:
				f.Set(reflect.ValueOf(null.IntFrom(v.(int64))))
			case time.Time:
				f.Set(reflect.ValueOf(null.TimeFrom(v.(time.Time))))
			case float32:
				f.Set(reflect.ValueOf(null.FloatFrom(float64(v.(float32)))))
			case float64:
				f.Set(reflect.ValueOf(null.FloatFrom(v.(float64))))
			default:
				f.Set(reflect.ValueOf(v))
			}
		}
	}
	return nil
}

//Struct数据转换为Map结构
func TransfStruct2Map(obj interface{}, keys []string) map[string]interface{} {
	m := make(map[string]interface{})
	for _, k := range keys {
		m[k] = nil
	}
	rType := reflect.TypeOf(obj)
	rVal := reflect.ValueOf(obj)
	if rType.Kind() == reflect.Ptr {
		// 传入的inStructPtr是指针，需要.Elem()取得指针指向的value
		rType = rType.Elem()
		rVal = rVal.Elem()
	}
	fieldNum := rType.NumField()
	for i := 0; i < fieldNum; i++ {
		n := rType.Field(i).Name
		if len(keys) == 0 {
			m[n] = rVal.Field(i).Interface()
		} else if _, ok := m[n]; ok {
			m[n] = rVal.Field(i).Interface()
		}
	}
	return m
}

//过滤map对象的nil值属性
func FilterMapFieldNull(sdata map[string]interface{}) (ddata map[string]interface{}) {
	ddata = make(map[string]interface{})

	for _key, _val := range sdata {
		if _val != nil {
			ddata[_key] = _val
		}
	}

	return
}

//根据key转换map对象string值为int值
func ConvToInt32ByKey(data map[string]interface{}, keys ...string) {
	for _, key := range keys {
		if v, ok := data[key]; ok {
			if s, ok := v.(string); ok {
				intVal, _ := strconv.ParseInt(s, 10, 32)
				data[key] = int32(intVal)
			} else if s, ok := v.(float64); ok {
				data[key] = int32(s)
			}
		}
	}
}

//根据key转换map对象string值为int64值
func ConvToInt64ByKey(data map[string]interface{}, keys ...string) {
	for _, key := range keys {
		if v, ok := data[key]; ok {
			if s, ok := v.(string); ok {
				intVal, _ := strconv.ParseInt(s, 10, 64)
				data[key] = intVal
			} else if s, ok := v.(float64); ok {
				data[key] = int64(s)
			}
		}
	}
}

//根据key转换map对象string值为float64值
func ConvToFloat64ByKey(data map[string]interface{}, keys ...string) {
	for _, key := range keys {
		if v, ok := data[key]; ok {
			if s, ok := v.(string); ok {
				fltVal, _ := strconv.ParseFloat(s, 64)
				data[key] = fltVal
			}
		}
	}
}

//根据key转换map对象string值为time.Time值
func ConvToTimeByKey(data map[string]interface{}, local *time.Location, keys ...string) {
	for _, key := range keys {
		if v, ok := data[key]; ok {
			if s, ok := v.(string); ok {
				if s != "" {
					tmpTime, err := StrToTime(s, local)
					if err == nil && tmpTime.Unix() > 0 {
						data[key] = tmpTime
						continue
					}
				}

				data[key] = nil
			}
		}
	}
}

//判断有效的配置文件路径并返回
func GetPathValid(v ...string) (string, error) {
	var err error = nil

	for _, filename := range v {
		_, err = os.Stat(filename)
		if err == nil {
			return filename, nil
		}
		if os.IsExist(err) {
			return filename, nil
		}
	}

	return "", err
}

//字符串转换为时间
func StrToTime(str string, local *time.Location) (time.Time, error) {
	if local == nil {
		local = TIMEZONE
	}

	ret, err := time.ParseInLocation("2006-1-2 15:4:5", str, local)
	if err != nil {
		ret, err = time.ParseInLocation("2006-1-2", str, local)
		if err != nil {
			ret, err = time.Parse(FORMATSQLTIME, str)
			if err != nil {
				ret, err = time.Parse(FORMATMSSQLTIME, str)
			}
		}
	}

	return ret, err
}
