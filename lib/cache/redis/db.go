package redis

import (
	"errors"
	"github.com/go-redis/redis"
	"github.com/maclon-lee/golanglib/lib/config"
	logger "github.com/maclon-lee/golanglib/lib/log"
	"time"
)

var rdb *redis.Client

/*
* 初始配置和连接
*/
func init() {
	if !config.IsSet("redis") {
		logger.Errorf("Redis连接未配置")
		return
	}

	cfglist := config.GetSubConfig("redis")
	if cfglist != nil {
		rdb = redis.NewClient(&redis.Options{
			Addr:       cfglist.GetString("addr"),
			Password:   cfglist.GetString("password"),
			DB:         cfglist.GetInt("database"),
			MaxRetries: 3,
		})

		if rdb == nil {
			logger.Errorf("Error creating redis client")
			return
		}
	}
}

//Ping
func Ping() (string, error) {
	if rdb == nil {
		return "error", errors.New("redis init: fail ")
	}

	return rdb.Ping().Result()
}

//缓存key-value数据
func Set(key string, value interface{}, expiration time.Duration) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	err := rdb.Set(key, value, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

//获取key-value数据
func Get(key string) (result string, err error) {
	if rdb == nil {
		return "", errors.New("redis init: fail ")
	}

	result, err = rdb.Get(key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return
}

//缓存key-value数据，并返回key的旧值
func GetSet(key string, value interface{}) (result string, err error) {
	if rdb == nil {
		return "", errors.New("redis init: fail ")
	}

	result, err = rdb.GetSet(key, value).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return
}

//当key不存在时才缓存key-value数据（返回是否保存了缓存）
func SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	if rdb == nil {
		return false, errors.New("redis init: fail ")
	}

	return rdb.SetNX(key, value, expiration).Result()
}

//删除缓存数据（返回删除数量）
func Del(keys ...string) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.Del(keys...).Err()
}

//判断key是否存在（返回存在数量）
func Exists(keys ...string) (int64, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.Exists(keys...).Result()
}

//对key的value值增量加1（整型，返回增量后的值）
func Incr(key string) (int64, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.Incr(key).Result()
}

//对key的value值增量加指定值（整型，返回增量后的值）
func IncrBy(key string, value int64) (int64, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.IncrBy(key, value).Result()
}

//对key的value值增量加指定值（浮点型，返回增量后的值）
func IncrByFloat(key string, value float64) (float64, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.IncrByFloat(key, value).Result()
}

//对key的value值减量减1（整型，返回减量后的值）
func Decr(key string) (int64, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.Decr(key).Result()
}

//对key的value值减量减指定值（整型，返回减量后的值）
func DecrBy(key string, decrement int64) (int64, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.DecrBy(key, decrement).Result()
}

//从key的列表值中移出并获取列表的第一个元素，如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止。
func BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	if rdb == nil {
		return nil, errors.New("redis init: fail ")
	}

	return rdb.BLPop(timeout, keys...).Result()
}

//从key的列表值中移出并获取列表的最后一个元素，如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止。
func BRPop(timeout time.Duration, keys ...string) ([]string, error) {
	if rdb == nil {
		return nil, errors.New("redis init: fail ")
	}

	return rdb.BRPop(timeout, keys...).Result()
}

//从source列表中弹出一个值，将弹出的元素插入到destination列表中并返回它； 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止。
func BRPopLPush(source string, destination string, timeout time.Duration) (string, error) {
	if rdb == nil {
		return "", errors.New("redis init: fail ")
	}

	result, err := rdb.BRPopLPush(source, destination, timeout).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return result, nil
}

//通过索引获取列表中的元素
func LIndex(key string, index int64) (string, error) {
	if rdb == nil {
		return "", errors.New("redis init: fail ")
	}

	result, err := rdb.LIndex(key, index).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return result, nil
}

//在key列表的pivot元素前或者后插入value元素（op取值范围为：BEFORE | AFTER）
func LInsert(key string, op string, pivot interface{}, value interface{}) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.LInsert(key, op, pivot, value).Err()
}

//获取列表长度
func LLen(key string) (int64, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.LLen(key).Result()
}

//移出并获取列表的第一个元素
func LPop(key string) (string, error) {
	if rdb == nil {
		return "", errors.New("redis init: fail ")
	}

	result, err := rdb.LPop(key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return result, nil
}

//将一个或多个值插入到列表头部
func LPush(key string, values ...interface{}) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.LPush(key, values...).Err()
}

//将一个值插入到已存在的列表头部
func LPushX(key string, value interface{}) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.LPushX(key, value).Err()
}

//获取列表指定范围内的元素
func LRange(key string, start int64, stop int64) ([]string, error) {
	if rdb == nil {
		return nil, errors.New("redis init: fail ")
	}

	return rdb.LRange(key, start, stop).Result()
}

/*
* 移除列表元素
*
* count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
* count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
* count = 0 : 移除表中所有与 value 相等的值。
*/
func LRem(key string, count int64, value interface{}) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.LRem(key, count, value).Err()
}

//通过索引index设置列表元素的value值
func LSet(key string, index int64, value interface{}) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.LSet(key, index, value).Err()
}

//对一个列表进行修剪(trim)，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
func LTrim(key string, start int64, stop int64) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.LTrim(key, start, stop).Err()
}

//移除列表的最后一个元素，返回值为移除的元素。
func RPop(key string) (string, error) {
	if rdb == nil {
		return "", errors.New("redis init: fail ")
	}

	result, err := rdb.RPop(key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return result, nil
}

//移除source列表的最后一个元素，并将该元素添加到destination列表并返回
func RPopLPush(source string, destination string) (string, error) {
	if rdb == nil {
		return "", errors.New("redis init: fail ")
	}

	result, err := rdb.RPopLPush(source, destination).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return result, nil
}

//在列表中添加一个或多个值
func RPush(key string, values ...interface{}) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.RPush(key, values...).Err()
}

//为已存在的列表添加值
func RPushX(key string, value interface{}) error {
	if rdb == nil {
		return errors.New("redis init: fail ")
	}

	return rdb.RPushX(key, value).Err()
}

//获取key的有效期
func TTL(key string) (time.Duration, error) {
	if rdb == nil {
		return 0, errors.New("redis init: fail ")
	}

	return rdb.TTL(key).Result()
}

//返回客户端引擎
func Engine() *redis.Client {
	return rdb
}