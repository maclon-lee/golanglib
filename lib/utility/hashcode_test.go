package utility

import (
	"reflect"
	"testing"
	"time"
)

func TestGetHashcode(t *testing.T) {
	var i interface{}
	i = test{A: 1, B: "fileb", C: time.Now()}
	v := reflect.ValueOf(i).FieldByName("A")
	t.Log(GetHashcode(v))
	v = reflect.ValueOf(i).FieldByName("B")
	t.Log(GetHashcode(v))
	v = reflect.ValueOf(i).FieldByName("C")
	t.Log(GetHashcode(v))
}

type test struct {
	A int
	B string
	C time.Time
}

func TestConvToIntBykey(t *testing.T) {
	m := make(map[string]interface{})
	m["a"]=nil
	_,ok:=m["a"]
	t.Log(ok)

}
