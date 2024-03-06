package sql

import (
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"github.com/maclon-lee/golanglib/lib/config"
	logger "github.com/maclon-lee/golanglib/lib/log"
	"github.com/maclon-lee/golanglib/lib/utility"
	"reflect"
	"time"
	"xorm.io/xorm"
)

//数据库上下文
type DbContext struct {
	db            *xorm.Engine
	TableConfs    []SplitTableConf `mapstructure:"splitTables"`
	Name          string           `mapstructure:"name"`
	Driver        string           `mapstructure:"driver"`
	ConnectString string           `mapstructure:"str"`
}
type SplitTableConf struct {
	TableName string   `mapstructure:"tableName"`
	Policies  []Policy `mapstructure:"policy"`
}
type Policy struct {
	Column string `mapstructure:"column"`
	Count  int    `mapstructure:"count"`
}

//批量SQL请求参数列表
type BatchSqlReq struct {
	Mode int // 0为Exec(Sql和Args), 1为InsertOne(Bean), 2为Update（Bean和Condi）
	Sql  string
	Args []interface{}
	Bean interface{}
	Condi interface{}
}

var dtxs map[string]*DbContext
var _default *DbContext
var isInit = false
var dbConf *viper.Viper

//构造数据库引擎
func newEngine(driver string, connectString string) (*xorm.Engine, error) {
	var db *xorm.Engine
	var err error

	//支持的数据库引擎
	switch driver {
	case "mysql":
		db, err = xorm.NewEngine("mysql", connectString)
	case "mssql":
		db, err = NewMssqlDriver(connectString)
	case "postgres":
		db, err = NewPostgresDriver(connectString)
	default:
		err = errors.New("不支持的数据库驱动:" + driver)
	}

	if err != nil {
		return nil, err
	}

	if dbConf != nil && dbConf.IsSet(driver) {
		setting := dbConf.Sub(driver)
		db.SetConnMaxLifetime(setting.GetDuration("maxLifetime") * time.Second)
		db.SetMaxIdleConns(setting.GetInt("maxIdle"))
		db.SetMaxOpenConns(setting.GetInt("maxOpen"))
	}
	if config.Env == "dev" {
		db.ShowSQL(true)
	}

	return db, nil
}

//初始化数据库上下文
func init() {
	if isInit {
		return
	}
	isInit = true

	if !config.IsSet("dbs") {
		return
	}

	var cfgList []DbContext
	dbConf = config.GetSubConfig("dbs")
	err := dbConf.UnmarshalKey("db", &cfgList)
	if err != nil {
		panic(fmt.Errorf("数据库配置错误:%s", err))
	}

	dtxs = make(map[string]*DbContext, 0)

	for i, ctx := range cfgList {
		db, err := newEngine(ctx.Driver, ctx.ConnectString)
		if err != nil {
			logger.Errorf("数据库连接:%s, 错误:%s", ctx.Name, err)
			continue
		}

		c := &cfgList[i]
		c.db = db
		dtxs[ctx.Name] = c
		if _default == nil {
			_default = c
		}
	}
}

/*
* 获取DB操作对象
* 参数dbKey：对应为config.toml中dbs.db的name值
*/
func GetContext(dbKey string) (*DbContext, error) {
	if db, ok := dtxs[dbKey]; ok {
		return db, nil
	}
	return nil, errors.New("No connection with this key ")
}

/*
* 获取默认DB操作对象
* 对应为config.toml中第一个有效的dbs.db配置
*/
func GetDefaultContext() *DbContext {
	return _default
}

/*
* 构建DB操作对象
*
* param  driver         数据库引擎（mysql, mssql, postgres）
* param  connectString  连接字符串（标准连接URL）
* return DB操作对象
 */
func NewContext(driver string, connectString string) (*DbContext, error) {
	db, err := newEngine(driver, connectString)
	if err != nil {
		return nil, err
	}
	return &DbContext{
		db:            db,
		TableConfs:    nil,
		Name:          "Custom",
		Driver:        driver,
		ConnectString: connectString,
	}, nil
}

/*
* 根据分表配置规则获取分表序号
*
* param  tbname  表标识名称,对应为config.toml中dbs.db的name值
* param  field   分表字段名
* param  value   分表字段值
*
* return 返回分表序号
 */
func (dtx DbContext) GetTableIdx(tbname string, field string, value interface{}) (int32, error) {
DoneLoop:
	for _, c := range dtx.TableConfs {
		if c.TableName == tbname {
			for _, p := range c.Policies {
				if p.Column == field {
					hashValue := utility.GetHashcode(value)
					if hashValue == 0 {
						return -1, fmt.Errorf("必须提供字段值:%s", p.Column)
					} else {
						if p.Count > 1 {
							hashValue = hashValue % int32(p.Count)
						}

						return hashValue, nil
					}

					break DoneLoop
				}
			}
			break DoneLoop
		}
	}

	return -1, nil
}

/*
* 根据分表配置规则获取分表后的表名
*
* param  entity  数据对象, 数据结构示例如下
*   type Entity struct {
*	   ID int64 `xorm:"ID"`  //标签xorm定义表字段名称
*   }
*
* return 返回表名
 */
func (dtx DbContext) GetTableName(entity interface{}) (string, error) {
	tableName := dtx.db.TableName(entity)
	for _, c := range dtx.TableConfs {
		if c.TableName == tableName {
			for _, p := range c.Policies {
				v := reflect.ValueOf(entity)
				if v.Kind() == reflect.Ptr {
					v = v.Elem()
				}
				hashValue := utility.GetHashcode(v.FieldByName(p.Column))
				if hashValue == 0 {
					return "", fmt.Errorf("必须提供字段参数:%v", p.Column)
				} else {
					if p.Count > 1 {
						hashValue = hashValue % int32(p.Count)
					}
					tableName = fmt.Sprintf("%v_%d", tableName, hashValue)
				}
			}
			break
		}
	}
	return tableName, nil
}

/*
* 根据分表配置规则获取所有表名
*
* param  entity  数据对象, 数据结构示例如下
*   type Entity struct {
*	   ID int64 `xorm:"ID"`  //标签xorm定义表字段名称
*   }
*
* return 返回表名列表
 */
func (dtx DbContext) GetAllTableName(entity interface{}) ([]string, error) {
	tableName := dtx.db.TableName(entity)
	var tables [][]string
	tables = append(tables, []string{tableName})
	for _, c := range dtx.TableConfs {
		if c.TableName == tableName {
			for _, p := range c.Policies {
				var temp []string
				for _, n := range tables[len(tables)-1] {
					if p.Count == 0 {
						v := reflect.ValueOf(entity)
						if v.Kind() == reflect.Ptr {
							v = v.Elem()
						}
						hashValue := utility.GetHashcode(v.FieldByName(p.Column))
						if hashValue == 0 {
							return nil, fmt.Errorf("必须提供字段参数:%v", p.Column)
						}
						temp = append(temp, fmt.Sprintf("%v_%v", n, hashValue))
					} else {
						for i := 0; i < p.Count; i++ {
							temp = append(temp, fmt.Sprintf("%v_%v", n, i))
						}
					}
				}
				tables = append(tables, temp)
			}
			break
		}
	}

	return tables[len(tables)-1], nil
}

/*
* 根据ID查询数据记录
*
* param  entity  返回数据对象, 数据结构示例如下
*   type Entity struct {
*	   ID int64 `xorm:"ID"`  //标签xorm定义表字段名称
*   }
* param  id  ID值列表
*
* return 是否异常
 */
func (dtx DbContext) Get(entity interface{}, id ...interface{}) error {
	var (
		tbName string
		err error
	)

	switch entity.(type) {
	case string:
		tbName = entity.(string)
	default:
		tbName, err = dtx.GetTableName(entity)
		if err != nil {
			return err
		}
	}

	_, err = dtx.db.Table(tbName).ID(id).Get(entity)
	return err
}

/*
* 插入数据，支持批量和单个
*
* param  entities  入库数据，单个数据结构示例如下
*   type Entity struct {
*	   ID int64 `xorm:"ID"`  //标签xorm定义表字段名称
*   }
*
* return 入库成功数量
*/
func (dtx DbContext) Inserts(entities ...interface{}) (int64, error) {
	return dtx.ImportData(false, 0, entities...)
}

/*
* 批量导入数据
*
* param  isIdentityInsert 是否包含插入自增类型字段值
* param  batchNum         单次批量入库数量
* param  entities         入库数据，单个数据结构示例如下
*   type Entity struct {
*	   ID int64 `xorm:"ID"`  //标签xorm定义表字段名称
*   }
*
* return 入库成功数量
 */
func (dtx DbContext) ImportData(isIdentityInsert bool, batchNum int, entities ...interface{}) (int64, error) {
	if len(entities) == 0 {
		return 0, errors.New("参数不能为空")
	}

	if len(entities) == 1 {
		tbName, err := dtx.GetTableName(entities[0])
		if err != nil {
			return 0, errors.New("GetTableName err:" + err.Error())
		}

		return dtx.db.Table(tbName).InsertOne(entities[0])
	}

	//先按分表规则分组
	group := make(map[string][]interface{})
	for _, bean := range entities {
		if tbn, err := dtx.GetTableName(bean); err != nil {
			return 0, errors.New("GetTableName err:" + err.Error())
		} else {
			var temp []interface{}
			if _beans, ok := group[tbn]; ok {
				temp = _beans
			}
			temp = append(temp, bean)
			group[tbn] = temp
		}
	}

	var n int64 = 0
	session := dtx.db.NewSession()
	defer session.Close()

	err := session.Begin()
	if err != nil {
		return 0, errors.New("session.Begin err:" + err.Error())
	}

	_bnum := 1000
	if batchNum > 0 {
		_bnum = batchNum
	}

	for k, v := range group {
		if isIdentityInsert && dtx.Driver == "mssql" {
			session.Exec(fmt.Sprintf("SET IDENTITY_INSERT %s on", k))
			defer session.Exec(fmt.Sprintf("SET IDENTITY_INSERT %s off", k))
		}

		_vlen := len(v)
		for j := 0; j < _vlen; j += _bnum {
			if j+_bnum > _vlen {
				_bnum = _vlen - j
			}

			session.Table(k)
			r, err := session.Insert(v[j : j+_bnum])
			if err != nil {
				_ = session.Rollback()
				return 0, errors.New("session.Insert err:" + err.Error())
			}
			n += r
		}
	}

	err = session.Commit()
	if err != nil {
		_ = session.Rollback()
		return 0, errors.New("session.Commit err:" + err.Error())
	}

	return n, nil
}

/*
* 更新数据
*
* param  fullTableName  表名，传空值时自动获取表名
* param  entity         入库数据，单个数据结构示例如下
*   type Entity struct {
*	   ID int64 `xorm:"ID"`  //标签xorm定义表字段名称
*   }
* param  condi          更新条件，支持map或struct类型
*
* return 入库成功数量
 */
func (dtx DbContext) Update(fullTableName string, entity interface{}, condi ...interface{}) (int64, error) {
	var err error
	tbName := fullTableName
	if tbName == "" {
		tbName, err = dtx.GetTableName(entity)
		if err != nil {
			return 0, err
		}
	}

	return dtx.db.Table(tbName).Update(entity, condi...)
}

/*
* 批量执行SQL
*
* param  BatchSqlReq  SQL对象，支持单个对象插入、自定义SQL语句、单个对象更新等
*
* return 入库成功数量
 */
func (dtx DbContext) BatchExec(req []BatchSqlReq) (int64, error) {
	if len(req) == 0 {
		return 0, errors.New("参数不能为空")
	}

	var n int64 = 0
	session := dtx.db.NewSession()
	defer session.Close()

	err := session.Begin()
	if err != nil {
		return 0, errors.New("session.Begin err:"+err.Error())
	}

	for _, _sql := range req {
		if _sql.Mode == 0 {
			var sqlOrArgs []interface{}
			sqlOrArgs = append(sqlOrArgs, _sql.Sql)
			sqlOrArgs = append(sqlOrArgs, _sql.Args...)

			res, err := session.Exec(sqlOrArgs...)
			if err != nil {
				_ = session.Rollback()
				return 0, errors.New("session.Exec err:"+err.Error())
			}

			_n, _ := res.RowsAffected()
			n += _n
		} else if _sql.Mode == 1 {
			tbName, err := dtx.GetTableName(_sql.Bean)
			if err != nil {
				_ = session.Rollback()
				return 0, errors.New("GetTableName err:"+err.Error())
			}

			session.Table(tbName)
			_n, err := session.InsertOne(_sql.Bean)
			if err != nil {
				_ = session.Rollback()
				return 0, errors.New("session.InsertOne err:"+err.Error())
			}
			n += _n
		} else if _sql.Mode == 2 {
			tbName, err := dtx.GetTableName(_sql.Bean)
			if err != nil {
				_ = session.Rollback()
				return 0, errors.New("GetTableName err:"+err.Error())
			}

			session.Table(tbName)
			_n, err := session.Update(_sql.Bean, _sql.Condi)
			if err != nil {
				_ = session.Rollback()
				return 0, errors.New("session.Update err:"+err.Error())
			}
			n += _n
		}
	}

	err = session.Commit()
	if err != nil {
		_ = session.Rollback()
		return 0, errors.New("session.Commit err:"+err.Error())
	}

	return n, nil
}

/*
* 删除数据
*
* param  fullTableName  表名，传空值时自动获取表名
* param  entity         删除条件，支持map或struct类型
*
* return 删除成功数量
 */
func (dtx DbContext) Delete(fullTableName string, entity interface{}) (int64, error) {
	var err error
	tbName := fullTableName
	if tbName == "" {
		tbName, err = dtx.GetTableName(entity)
		if err != nil {
			return 0, nil
		}
	}

	return dtx.db.Table(tbName).Delete(entity)
}

/*
* 自定义SQL查询
*
* param  rowsSlicePtr   返回记录行数据对象, 支持[]struct, []map[string]interface{}类型，struct结构示例如下
*   type Entity struct {
*	   ID int64 `xorm:"ID"`  //标签xorm定义表字段名称
*   }
* param  sql            SQL语句
* param  args           SQL参数
*
* return 入库成功数量
 */
func (dtx DbContext) Query(rowsSlicePtr interface{}, sql string, args ...interface{}) error {
	return dtx.db.SQL(sql, args...).Find(rowsSlicePtr)
}

/*
* 执行自定义SQL
*
* param  sql            SQL语句
* param  args           SQL参数
*
* return 执行成功数量
 */
func (dtx DbContext) Exec(sql string, args ...interface{}) (int64, error) {
	var sqlOrArgs []interface{}
	sqlOrArgs = append(sqlOrArgs, sql)
	sqlOrArgs = append(sqlOrArgs, args...)

	res, err := dtx.db.Exec(sqlOrArgs...)
	if err == nil {
		_n, _ := res.RowsAffected()
		return _n, nil
	}
	return 0, err
}

//返回客户端引擎
func (dtx DbContext) Engine() *xorm.Engine {
	return dtx.db
}

//关闭连接
func (dtx DbContext) Close() {
	dtx.db.Close()
}
