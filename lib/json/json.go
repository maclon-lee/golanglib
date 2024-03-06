package json

import (
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
	"github.com/maclon-lee/golanglib/lib/utility"
	"io"
	"net/http"
	"time"
	"unsafe"
)

var gjson jsoniter.API
var jsonContentType = []string{"application/json; charset=utf-8"}
// JSON contains the given interface object.
type JsoniterJSON struct {
	Data interface{}
}


/**
封装jsoniter json 库
**/
func init() {
	gjson = jsoniter.ConfigCompatibleWithStandardLibrary
	//弱类型转换
	extra.RegisterFuzzyDecoders()
	//自定义日期格式
	jsoniter.RegisterTypeDecoderFunc("time.Time", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		str := iter.ReadString()
		if str == "" {
			return
		}
		t, err := utility.StrToTime(str, time.Local)
		if err != nil {
			iter.Error = err
			return
		}
		*((*time.Time)(ptr)) = t
	})
	jsoniter.RegisterTypeEncoderFunc("time.Time", func(ptr unsafe.Pointer, stream *jsoniter.Stream) {
		t := *((*time.Time)(ptr))

		if !t.IsZero() && t.Unix() > 0 {
			t = t.Local()
			str := t.Format(utility.FORMATDATETIME)
			stream.WriteString(str)
		} else {
			stream.WriteNil()
		}
	}, func(ptr unsafe.Pointer) bool {
		t := *((*time.Time)(ptr))
		if t.IsZero() {
			return true
		}

		return false
	})
}

//json序列化为string
func MarshalToString(v interface{}) (string, error) {
	return gjson.MarshalToString(v)
}

//string反序列化json
func UnmarshalFromString(str string, v interface{}) error {
	return gjson.UnmarshalFromString(str, v)
}

//json序列化为[]byte
func Marshal(v interface{}) ([]byte, error) {
	return gjson.Marshal(v)
}

//[]byte反序列化json
func Unmarshal(data []byte, v interface{}) error {
	return gjson.Unmarshal(data, v)
}

//读取数据流
func NewDecoder(reader io.Reader) *jsoniter.Decoder {
	return gjson.NewDecoder(reader)
}

//写入数据流
func NewEncoder(writer io.Writer) *jsoniter.Encoder {
	return gjson.NewEncoder(writer)
}

// Render (JSON) writes data with custom ContentType.
func (r JsoniterJSON) Render(w http.ResponseWriter) (err error) {
	if err = WriteJSON(w, r.Data); err != nil {
		panic(err)
	}
	return
}

// WriteContentType (JSON) writes JSON ContentType.
func (r JsoniterJSON) WriteContentType(w http.ResponseWriter) {
	writeContentType(w, jsonContentType)
}

// WriteJSON marshals the given interface object and writes it with custom ContentType.
func WriteJSON(w http.ResponseWriter, obj interface{}) error {
	writeContentType(w, jsonContentType)
	jsonBytes, err := gjson.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = w.Write(jsonBytes)
	if err != nil {
		return errors.New(fmt.Sprintf("%s. Response:%s", err, jsonBytes))
	}
	return nil
}

func writeContentType(w http.ResponseWriter, value []string) {
	header := w.Header()
	if val := header["Content-Type"]; len(val) == 0 {
		header["Content-Type"] = value
	}
}
