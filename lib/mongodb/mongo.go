package mongodb

import (
	"context"
	"errors"
	"fmt"
	"github.com/maclon-lee/golanglib/lib/config"
	logger "github.com/maclon-lee/golanglib/lib/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"strings"
)

type MgoContext struct {
	mdb *mongo.Client
	ctx context.Context
	db  *mongo.Database

	dbName  string
	Name    string `mapstructure:"name"`
	Address string `mapstructure:"address"`
}

//mongo对象集合
var mtxs map[string]*MgoContext
var isInit = false

//构造MongoDB引擎
func newEngine(address string) (string, context.Context, *mongo.Client, error) {
	startIdx := strings.LastIndex(address, "/")
	endIdx := strings.LastIndex(address, "?")
	if startIdx == -1 {
		return "", nil, nil, errors.New("mongo配置错误, 格式为:mongodb://user:pass@host:port/database?options")
	}

	database := ""
	if endIdx == -1 {
		database = address[startIdx+1:]
	} else {
		database = address[startIdx+1 : endIdx]
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(address))
	if err != nil {
		return "", nil, nil, err
	}

	return database, ctx, client, nil
}

/*
* 初始配置和连接
 */
func init() {
	if isInit {
		return
	}
	isInit = true

	if config.IsSet("mongo") {
		var cfgList []MgoContext
		mConf := config.GetSubConfig("mongo")
		err := mConf.UnmarshalKey("db", &cfgList)
		if err != nil {
			panic(fmt.Errorf("mongo配置错误:%s", err))
		}

		mtxs = make(map[string]*MgoContext, 0)
		for i, mtx := range cfgList {
			database, ctx, client, err := newEngine(mtx.Address)
			if err != nil {
				logger.Errorf("Error creating mongo client: %s", err)
				continue
			}

			c := &cfgList[i]
			c.mdb = client
			c.ctx = ctx
			c.dbName = database
			mtxs[mtx.Name] = c
		}
	}
}

/*
* 获取mongo操作对象
*
* param  mKey    标识名称，对应为config.toml中mongo.db的name值
* param  dbName  数据库名称，传空值则用连接字符串中的数据库名
* return mongo操作对象
 */
func GetContext(mKey string, dbName string) (*MgoContext, error) {
	if mtx, ok := mtxs[mKey]; ok {
		if dbName == "" {
			dbName = mtx.dbName
		}
		if dbName == "" {
			return nil, errors.New("未配置数据库名")
		}

		return &MgoContext{
			mdb:     mtx.mdb,
			ctx:     mtx.ctx,
			db:      mtx.mdb.Database(dbName),
			dbName:  dbName,
			Name:    mtx.Name,
			Address: mtx.Address,
		}, nil
	}
	return nil, errors.New("No mongo client with this key ")
}

/*
* 构建mongo操作对象
*
* param  address  连接字符串，格式：mongodb://user:pass@host:port/database?options
* param  dbName   数据库名称，传空值则用连接字符串中的数据库名
* return mongo操作对象
*/
func NewContext(address string, dbName string) (*MgoContext, error) {
	database, ctx, client, err := newEngine(address)
	if err != nil {
		return nil, err
	}
	if dbName == "" {
		dbName = database
	}

	return &MgoContext{
		mdb:     client,
		ctx:     ctx,
		db:      client.Database(dbName),
		dbName:  dbName,
		Name:    "Custom",
		Address: address,
	}, nil
}

//Ping
func (mtx MgoContext) Ping() error {
	if mtx.mdb == nil {
		return errors.New("mongo init: fail ")
	}

	err := mtx.mdb.Ping(mtx.ctx, readpref.Primary())
	if err != nil {
		return err
	}
	return nil
}

/*
* 判断Collection是否存在
*
* param  tbName  Collection名称
* return 是否存在
 */
func (mtx MgoContext) CollectionExist(tbName string) (bool, error) {
	if mtx.mdb == nil {
		return false, errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)
	cursor, err := tb.Indexes().List(mtx.ctx)
	if err != nil {
		return false, err
	}
	defer cursor.Close(mtx.ctx)

	if cursor.Next(mtx.ctx) {
		return true, nil
	}

	return false, nil
}

/*
* 创建Collection
*
* param  tbName  Collection名称
* return 是否成功
*/
func (mtx MgoContext) CreateCollection(tbName string) error {
	if mtx.mdb == nil {
		return errors.New("mongo init: fail ")
	}

	err := mtx.db.CreateCollection(mtx.ctx, tbName)
	if err != nil {
		return err
	}

	return nil
}

/*
* 创建索引
*
* param  tbName      Collection名称
* param  uniqueKeys  唯一键（参与字段名）
* param  indexKeys   索引（参与字段名）
* return 是否成功
 */
func (mtx MgoContext) CreateIndex(tbName string, uniqueKeys [][]string, indexKeys [][]string) error {
	if mtx.mdb == nil {
		return errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)
	var models []mongo.IndexModel

	if len(uniqueKeys) > 0 {
		isUnique := true
		for _, item := range uniqueKeys {
			keys := bson.D{}
			for _, key := range item {
				keys = append(keys, bson.E{Key: key, Value: 1})
			}
			models = append(models, mongo.IndexModel{
				Keys: keys,
				Options: &options.IndexOptions{
					Unique: &isUnique,
				},
			})
		}
	}

	if len(indexKeys) > 0 {
		isUnique := false
		for _, item := range indexKeys {
			keys := bson.D{}
			for _, key := range item {
				keys = append(keys, bson.E{Key: key, Value: 1})
			}
			models = append(models, mongo.IndexModel{
				Keys: keys,
				Options: &options.IndexOptions{
					Unique: &isUnique,
				},
			})
		}
	}

	if len(models) > 0 {
		_, err := tb.Indexes().CreateMany(mtx.ctx, models)
		if err != nil {
			return err
		}
	} else {
		return errors.New("无索引创建")
	}

	return nil
}

/*
* 创建分区
*
* param  tbName      Collection名称
* param  shardKeys   分区（参与字段名）
* return 是否成功
 */
func (mtx MgoContext) CreateShard(dbName string, tbName string, shardKeys []string) error {
	if mtx.mdb == nil {
		return errors.New("mongo init: fail ")
	}
	if len(shardKeys) == 0 {
		return errors.New("无分区创建")
	}
	if mtx.dbName != "admin" {
		return errors.New("必须连admin库才能分片")
	}

	//开启分区
	enableSharding := bson.D{{Key: "enablesharding", Value: dbName}}
	err := mtx.db.RunCommand(mtx.ctx, enableSharding).Err()
	if err != nil {
		return err
	}

	//分区字段
	shardCollection := bson.D{}
	shardCollection = append(shardCollection, bson.E{Key: "shardcollection", Value: fmt.Sprintf("%s.%s", dbName, tbName)})
	keys := bson.M{}
	for _, key := range shardKeys {
		keys[key] = 1
	}
	shardCollection = append(shardCollection, bson.E{Key: "key", Value: keys})

	err = mtx.db.RunCommand(mtx.ctx, shardCollection).Err()
	if err != nil {
		return err
	}

	return nil
}

/*
* 插入数据，支持单个或批量
* bson类型，包名："go.mongodb.org/mongo-driver/bson"
*
* param  tbName  Collection名称
* param  datas   入库数据，支持struct或bson类型，struct结构示例如下
*  type Data struct {
*	 ID  int64 `bson:"ID,omitempty"`  //标签bson定义表字段名称
*  }
*
* return 入库成功数量
 */
func (mtx MgoContext) Inserts(tbName string, datas []interface{}) (int, error) {
	if mtx.mdb == nil {
		return 0, errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)

	result, err := tb.InsertMany(mtx.ctx, datas)
	if err != nil {
		return 0, err
	}

	return len(result.InsertedIDs), nil
}

/*
* 更新数据
* bson类型，包名："go.mongodb.org/mongo-driver/bson"
*
* param  tbName  Collection名称
* param  filter  更新条件, 仅支持bson类型，bson类型示例如下
*  示例1: bson.M{"Title": "The Polyglot Developer Podcast"}
*  示例2: bson.D{{"UpdateTime", bson.D{{"$lt", time.Now().Add(-5 * time.Minute)}}}}
*  (更多检索帮助，详见：https://docs.mongodb.com/manual/reference/operator/query/)
*
* param  data    更新数据，map类型
*                - map体对应的JSON格式为： {"key":"value"}
*                - key为字段名，value字段值
*
* param  isUpsert  是否不存在时插入数据
*
* return 入库成功数量
 */
func (mtx MgoContext) Update(tbName string, filter interface{}, data map[string]interface{}, isUpsert bool) (map[string]int64, error) {
	if mtx.mdb == nil {
		return nil, errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)

	_d := bson.M{}
	for _key, _val := range data {
		_d[_key] = _val
	}
	update := bson.D{
		{"$set", _d},
	}

	result, err := tb.UpdateMany(mtx.ctx, filter, update, &options.UpdateOptions{Upsert: &isUpsert})
	if err != nil {
		return nil, err
	}

	return map[string]int64{
		"creates": result.UpsertedCount,
		"updates": result.ModifiedCount,
		"matches": result.MatchedCount,
	}, nil
}

/*
* 删除数据
* bson类型，包名："go.mongodb.org/mongo-driver/bson"
*
* param  tbName  Collection名称
* param  filter  删除条件, 仅支持bson类型，bson类型示例如下
*  示例1: bson.M{"Title": "The Polyglot Developer Podcast"}
*  示例2: bson.D{{"UpdateTime", bson.D{{"$lt", time.Now().Add(-5 * time.Minute)}}}}
*  (更多检索帮助，详见：https://docs.mongodb.com/manual/reference/operator/query/)
*
* return 删除成功数量
 */
func (mtx MgoContext) Delete(tbName string, filter interface{}) (int64, error) {
	if mtx.mdb == nil {
		return 0, errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)

	result, err := tb.DeleteMany(mtx.ctx, filter)
	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

/*
* 检索数据
* bson类型，包名："go.mongodb.org/mongo-driver/bson"
*
* param  rowsSlicePtr   返回记录行数据对象, 支持[]struct, []map[string]interface{}类型，struct结构示例如下
*  type Data struct {
*	 ID  int64 `bson:"ID,omitempty"`  //标签bson定义表字段名称
*  }
* param  tbName         Collection名称
* param  filter         查询条件, 仅支持bson类型，bson类型示例如下
*  示例1: bson.M{"Title": "The Polyglot Developer Podcast"}
*  示例2: bson.D{{"UpdateTime", bson.D{{"$lt", time.Now().Add(-5 * time.Minute)}}}}
*  (更多检索帮助，详见：https://docs.mongodb.com/manual/reference/operator/query/)
*
* param  offset         记录开始序号（0开始）
* param  limit          返回记录条数
* param  sorts          排序map体
*                       - map体对应的JSON格式为： {"key":"type"}
*                       - key为字段名，type取值范围为：desc（降序）, asc（升序）
* return 返回是否异常
 */
func (mtx MgoContext) Search(rowsSlicePtr interface{}, tbName string, filter interface{}, offset int64, limit int64, sorts map[string]string) error {
	if mtx.mdb == nil {
		return errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)

	opt := &options.FindOptions{
		Limit: &limit,
		Skip:  &offset,
	}
	if sorts != nil {
		_s := bson.D{}
		for _key, _val := range sorts {
			_order := 1
			if strings.ToLower(_val) == "desc" {
				_order = -1
			}

			_s = append(_s, bson.E{Key: _key, Value: _order})
		}
		opt.SetSort(_s)
	}

	cursor, err := tb.Find(mtx.ctx, filter, opt)
	if err != nil {
		return err
	}
	defer cursor.Close(mtx.ctx)

	err = cursor.All(mtx.ctx, rowsSlicePtr)
	if err != nil {
		return err
	}
	return nil
}

/*
* 去重查询
* bson类型，包名："go.mongodb.org/mongo-driver/bson"
*
* param  tbName         Collection名称
* param  fieldName      结果字段名
* param  filter         查询条件, 仅支持bson类型，bson类型示例如下
*  示例1: bson.M{"Title": "The Polyglot Developer Podcast"}
*  示例2: bson.D{{"UpdateTime", bson.D{{"$lt", time.Now().Add(-5 * time.Minute)}}}}
*  (更多检索帮助，详见：https://docs.mongodb.com/manual/reference/operator/query/)
*
* return  返回去重后的结果字段值
 */
func (mtx MgoContext) Distinct(tbName string, fieldName string, filter interface{}) ([]interface{}, error) {
	if mtx.mdb == nil {
		return nil, errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)

	return tb.Distinct(mtx.ctx, fieldName, filter)
}

/*
* 统计记录数量
* bson类型，包名："go.mongodb.org/mongo-driver/bson"
*
* param  tbName         Collection名称
* param  filter         查询条件, 仅支持bson类型，bson类型示例如下
*  示例1: bson.M{"Title": "The Polyglot Developer Podcast"}
*  示例2: bson.D{{"UpdateTime", bson.D{{"$lt", time.Now().Add(-5 * time.Minute)}}}}
*  (更多检索帮助，详见：https://docs.mongodb.com/manual/reference/operator/query/)
*
* return 记录行数
 */
func (mtx MgoContext) Count(tbName string, filter interface{}) (int64, error) {
	if mtx.mdb == nil {
		return 0, errors.New("mongo init: fail ")
	}

	tb := mtx.db.Collection(tbName)

	return tb.CountDocuments(mtx.ctx, filter)
}

/*
* 分组统计
* bson类型，包名："go.mongodb.org/mongo-driver/bson"
*
* param  rowsSlicePtr   返回记录行数据对象, 支持[]struct, []map[string]interface{}类型，struct结构示例如下
*  type Data struct {
*	 ID  int64 `bson:"ID,omitempty"`  //标签bson定义表字段名称
*  }
* param  tbName         Collection名称
* param  filter         过滤条件, 仅支持bson类型，bson类型示例如下
*  示例1: bson.M{"Title": "The Polyglot Developer Podcast"}
*  示例2: bson.D{{"UpdateTime", bson.D{{"$lt", time.Now().Add(-5 * time.Minute)}}}}
*  (更多检索帮助，详见：https://docs.mongodb.com/manual/reference/operator/query/)
*
* param  groupBy        分组字段名, 对应返回记录行数据对象的字段名为_id, 参考文档（Field=_id）：https://docs.mongodb.com/manual/reference/operator/aggregation/group/#pipe._S_group
* param  columnBy       分组统计项map体, map体对应的JSON格式为： {"name":{"type":"key"}}
*                       - name自定义统计项名称
*                       - type取值范围为：$avg, $first, $last, $max, $min, $sum, 其它详见官方文档：https://docs.mongodb.com/manual/reference/operator/aggregation/group/#accumulator-operator
*                       - key为字段名(也可为bson类型定义), 参考文档（Field=field）：https://docs.mongodb.com/manual/reference/operator/aggregation/group/#examples
*
* param  sorts          排序map体
*                       - map体对应的JSON格式为： {"key":"type"}
*                       - key为字段名，type取值范围为：desc（降序）, asc（升序）
*
* return  返回是否异常
 */
func (mtx MgoContext) Group(rowsSlicePtr interface{}, tbName string, filter interface{}, groupBy interface{}, columnBy map[string]map[string]interface{}, sorts map[string]string) error {
	if mtx.mdb == nil {
		return errors.New("mongo init: fail ")
	}

	pipeline := mongo.Pipeline{}
	columnStage := bson.D{}

	if filter != nil {
		pipeline = append(pipeline, bson.D{
			{"$match", filter},
		})
	}

	if groupBy != nil {
		columnStage = append(columnStage, bson.E{
			Key: "_id", Value: groupBy,
		})
	}

	for _name, _item := range columnBy {
		for _type, _key := range _item {
			columnStage = append(columnStage, bson.E{
				Key: _name, Value: bson.M{
					_type: _key,
				},
			})
		}
	}

	pipeline = append(pipeline, bson.D{
		{"$group", columnStage},
	})

	if sorts != nil {
		_s := bson.D{}
		for _key, _val := range sorts {
			_order := 1
			if strings.ToLower(_val) == "desc" {
				_order = -1
			}

			_s = append(_s, bson.E{Key: _key, Value: _order})
		}

		pipeline = append(pipeline, bson.D{{"$sort", _s}})
	}

	tb := mtx.db.Collection(tbName)

	cursor, err := tb.Aggregate(mtx.ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(mtx.ctx)

	err = cursor.All(mtx.ctx, rowsSlicePtr)
	if err != nil {
		return err
	}
	return nil
}

//关闭连接
func (mtx MgoContext) Close() {
	if mtx.mdb == nil {
		return
	}
	mtx.mdb.Disconnect(mtx.ctx)
}
