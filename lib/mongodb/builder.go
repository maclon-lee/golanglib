package mongodb

import (
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
)

//Filter builder
type Filter struct {
	AndFilter bson.D
	OrFilter  bson.A
}

func NewFilter() *Filter {
	return &Filter{
		AndFilter: bson.D{},
		OrFilter:  bson.A{},
	}
}
func (w Filter) Build() bson.D {
	result := bson.D{}
	if len(w.AndFilter) > 0 {
		result = append(result, w.AndFilter...)
	}
	if len(w.OrFilter) > 0 {
		result = append(result, bson.E{
			Key:   "$or",
			Value: w.OrFilter,
		})
	}
	return result
}

/*
* append与查询条件
*
* param  cond     是否加入条件
* param  field    字段名
* param  operate  操作符（eq, gt, gte, in, lt, lte, ne, nin），无需带$符号
*                 - eq：等于, gt：大于, gte：大于或等于, lt：小于, lte：小于或等于
*                 - ne：不等于, in：在数组中, nin：不在数组中
*                 - 其它详见：https://docs.mongodb.com/manual/reference/operator/query/
* param  args     字段值
*
* return Filter对象
*/
func (w *Filter) AppendAnd(cond bool, field string, operate string, args ...interface{}) *Filter {
	if cond {
		_if := filterHelper(field, operate, args...)
		w.AndFilter = append(w.AndFilter, _if)
	}
	return w
}

/*
* append或查询条件
*
* param  cond     是否加入条件
* param  field    字段名
* param  operate  操作符（eq, gt, gte, in, lt, lte, ne, nin），无需带$符号
*                 - eq：等于, gt：大于, gte：大于或等于, lt：小于, lte：小于或等于
*                 - ne：不等于, in：在数组中, nin：不在数组中
*                 - 其它详见：https://docs.mongodb.com/manual/reference/operator/query/
* param  args     字段值
*
* return Filter对象
 */
func (w *Filter) AppendOr(cond bool, field string, operate string, args ...interface{}) *Filter {
	if cond {
		_if := filterHelper(field, operate, args...)
		w.OrFilter = append(w.OrFilter, _if)
	}
	return w
}

func filterHelper(field string, operate string, args ...interface{}) bson.E {
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

	if len(_args) > 1 {
		return bson.E{
			Key:   field,
			Value: bson.D{{"$" + operate, _args}},
		}
	} else if len(_args) == 1 {
		return bson.E{
			Key:   field,
			Value: bson.D{{"$" + operate, _args[0]}},
		}
	}

	return bson.E{
		Key:   field,
		Value: bson.D{{"$" + operate, nil}},
	}
}
