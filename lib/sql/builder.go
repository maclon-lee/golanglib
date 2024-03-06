package sql

import (
	"fmt"
	"reflect"
	"strings"
)

//获取select字段名列表（source为空值时，取entity中的键名作为字段名）
func BuildSelect(source []string, entity interface{}) string {
	if len(source) == 0 {
		if entity == nil {
			return ""
		}
		v := reflect.ValueOf(entity)
		if v.Kind() == reflect.Ptr {
			v = reflect.Indirect(v)
		}
		t := v.Type()
		source = make([]string, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			source[i] = t.Field(i).Name
		}
	}
	return strings.Join(source, ",")
}

//Where builder
type Where struct {
	Sql  strings.Builder
	Args []interface{}
}

func NewWhere() *Where {
	return &Where{
		Sql:  strings.Builder{},
		Args: make([]interface{}, 0),
	}
}
func (w Where) Build() (string, []interface{}) {
	return w.Sql.String(), w.Args
}

//append SQL短句（不支持In 数组）
func (w *Where) AppendIf(cond bool, whereStr string, args ...interface{}) *Where {
	if cond {
		if w.Sql.Len() > 0 {
			if !strings.HasPrefix(strings.ToUpper(strings.Trim(whereStr, " ")), "AND") {
				w.Sql.WriteString(" AND ")
			}
		} else {
			w.Sql.WriteString(" WHERE ")
		}
		w.Sql.WriteString(whereStr)
		w.Args = append(w.Args, args...)
	}
	return w
}

//append in查询 数组
func (w *Where) InIf(cond bool, col string, args ...interface{}) *Where {
	if cond {
		var _args []interface{}
		if len(args) == 1 {
			v := reflect.ValueOf(args[0])
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			if v.Kind() == reflect.Slice {
				_args = make([]interface{}, v.Len())
				for i := 0; i < v.Len(); i++ {
					_args[i] = v.Index(i).Interface()
				}
			} else {
				_args = args
			}
		} else {
			_args = args
		}

		if w.Sql.Len() > 0 {
			w.Sql.WriteString(" AND ")
		} else {
			w.Sql.WriteString(" WHERE ")
		}
		w.Sql.WriteString(col)
		w.Sql.WriteString(" IN (")
		for i, _ := range _args {
			if i == 0 {
				w.Sql.WriteString("?")
			} else {
				w.Sql.WriteString(",?")
			}
		}
		w.Sql.WriteString(" ) ")
		w.Args = append(w.Args, _args...)
	}
	return w
}

//append 单表达式查询（不支持In 数组）
func (w *Where) ExprIf(cond bool, col string, symbol string, args ...interface{}) *Where {
	if cond {
		if w.Sql.Len() > 0 {
			w.Sql.WriteString(" AND ")
		} else {
			w.Sql.WriteString(" WHERE ")
		}

		w.Sql.WriteString(fmt.Sprintf(" %s %s ?", col, symbol))
		w.Args = append(w.Args, args...)
	}
	return w
}
