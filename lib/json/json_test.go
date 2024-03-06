package json

import (
	"testing"
	"time"
)

type Test struct {
	A int       `json:"a"`
	T time.Time `json:"t,omitempty"`
	T1 time.Time `json:"t1"`
}

func TestJson(t *testing.T) {
	json:=`{"a":"666","t":"2019-01-29T17:27:14+08:00","t1":"0001-01-01T00:00:00+08:05"}`
	var test Test
	err:= UnmarshalFromString(json,&test)
	if err!=nil{
		t.Error(err)
		t.SkipNow()
	}

	t.Log(test)
	json1,err:=MarshalToString(test)
	if err!=nil{
		t.Error(err)
		t.SkipNow()
	}
	t.Log(json1)

}

