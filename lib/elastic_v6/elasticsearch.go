package elastic_v6

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/maclon-lee/golanglib/lib/config"
	httpclient "github.com/maclon-lee/golanglib/lib/httpd/client"
	"github.com/maclon-lee/golanglib/lib/json"
	"net"
	"net/http"
	"strconv"

	//"reflect"
	"strings"
	"time"

	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
	esapi6 "github.com/elastic/go-elasticsearch/v6/esapi"

	logger "github.com/maclon-lee/golanglib/lib/log"
)

//批量存储参数（增、删、改）
var (
	TInsert = 1
	TUpdate = 2
	TDelete = 3
)
type TypeBatchData struct {
	Type int
	Doc  string
	Data interface{}
}

//ElasticSearch上下文
type EsContext struct {
	es *elasticsearch6.Client

	Name     string   `mapstructure:"name"`
	Address  []string `mapstructure:"address"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
}

//ES对象集合
var etxs map[string]*EsContext
var isInit = false

//构造ElasticSearch引擎
func newEngine(address []string, username string, password string) (*elasticsearch6.Client, error) {
	if len(address) == 0 {
		return nil, errors.New("Elasticsearch address not be empty ")
	}

	cfg := elasticsearch6.Config{
		Addresses: address,
		Username:  username,
		Password:  password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: 300 * time.Second,
			DialContext:           (&net.Dialer{Timeout: 300 * time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
		},
		Logger: &ESLogger{},
	}

	es, err := elasticsearch6.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return es, nil
}

/*
* 初始配置和连接
 */
func init() {
	if isInit {
		return
	}
	isInit = true

	if config.IsSet("es6") {
		var cfgList []EsContext
		dbConf := config.GetSubConfig("es6")
		err := dbConf.UnmarshalKey("db", &cfgList)
		if err != nil {
			panic(fmt.Errorf("ES配置错误:%s", err))
		}

		etxs = make(map[string]*EsContext, 0)

		for i, ctx := range cfgList {
			es, err := newEngine(ctx.Address, ctx.Username, ctx.Password)
			if err != nil {
				logger.Errorf("Error creating Elasticsearch client: %s", err)
				continue
			}

			c := &cfgList[i]
			c.es = es
			etxs[ctx.Name] = c
		}
	}
}

/*
* 获取ES操作对象
* 参数esKey：对应为config.toml中es.db的name值
 */
func GetContext(esKey string) (*EsContext, error) {
	if etx, ok := etxs[esKey]; ok {
		return etx, nil
	}
	return nil, errors.New("No Elasticsearch client with this key ")
}

/*
* 构建ES操作对象
*
* param  address  连接地址，格式：http://ip:port 或 https://ip:port
* param  username 连接用户名
* param  password 连接密码
* return ES操作对象
*/
func NewContext(address []string, username string, password string) (*EsContext, error) {
	es, err := newEngine(address, username, password)
	if err != nil {
		return nil, err
	}

	return &EsContext{
		es:       es,
		Name:     "Custom",
		Address:  address,
		Username: username,
		Password: password,
	}, nil
}

/*
* 获取ES节点信息
 */
func (etx EsContext) GetInfo() (result interface{}, err error) {
	if etx.es != nil {
		res, err := etx.es.Info()
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error GetInfo", res.Status()))
		} else {
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail ")
}

/*
* 判断index是否存在
* param  indexs    索引库名称，数组
* return 是否存在
 */
func (etx EsContext) CheckIndexExists(indexs []string) (bool, error) {
	if etx.es != nil {
		req := esapi6.IndicesExistsRequest{
			Index: indexs,
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return false, err
		}
		defer res.Body.Close()

		if res.IsError() || res.StatusCode != http.StatusOK {
			return false, nil
		}

		req2 := esapi6.IndicesStatsRequest{
			Index: indexs,
		}
		res2, err := req2.Do(context.Background(), etx.es)
		if err != nil {
			return false, err
		}
		defer res2.Body.Close()

		if res2.IsError() || res2.StatusCode != http.StatusOK {
			return false, nil
		}

		return true, nil
	}

	return false, errors.New("Elasticsearch init: fail ")
}

/*
* 创建index
* param  index   索引库名称
* param  types   创建index的字段类型map体（支持多级字段属性定义）
*                 - map体对应的JSON格式为： {"字段名":{"type": "类型"} | JSON}
*                 - type为定义类型的键名，类型取值范围：integer, long, text, keyword, 其它（详见：https://www.elastic.co/guide/en/elasticsearch/reference/6.8/mapping-types.html）
*                 - JSON为内嵌字段类型定义，支持多级字段属性定义
* param  settings 设置index配置项
*                 - map体对应的JSON格式为： {"属性名":"值"}
* return ES响应结果map体
 */
func (etx EsContext) CreateIndex(index string, documentType interface{}, types map[string]interface{}, settings map[string]string) (result interface{}, err error) {
	if etx.es != nil {
		var b strings.Builder

		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		b.WriteString("{")

		if settings != nil {
			b.WriteString(`"settings":`)

			setData, err := json.MarshalToString(settings)
			if err != nil {
				return nil, err
			}
			b.WriteString(setData)

			b.WriteString(",")
		}

		b.WriteString(fmt.Sprintf(`"mappings": {"%s": {"properties":`, _type))

		typeData, err := json.MarshalToString(types)
		if err != nil {
			return nil, err
		}
		b.WriteString(typeData)

		b.WriteString("}}}")

		req := esapi6.IndicesCreateRequest{
			Index:           index,
			Body:            strings.NewReader(b.String()),
			IncludeTypeName: nil,
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error CreateIndex req=%s", res.Status(), b.String()))
		} else {
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail ")
}

/*
* 插入文档数据
* param  index  索引库名称
* param  docid  文档ID，空字符串代表由系统生成ID
* param  data   数据结构体，标签json标识对应字段名，结构体类型举例：
*    type User struct {
*        Userid uint32  `json:"user_id"`
*    	 Amount float64 `json:"amount"`
*    }
* return ES响应结果map体
 */
func (etx EsContext) InsertData(index string, documentType interface{}, docid string, data interface{}) (result interface{}, err error) {
	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		reqdata, err := json.MarshalToString(data)
		if err != nil {
			return nil, err
		}

		req := esapi6.IndexRequest{
			Index:        index,
			DocumentType: _type,
			DocumentID:   docid,
			Body:         strings.NewReader(reqdata),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error InsertData req=%s", res.Status(), reqdata))
		} else {
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail ")
}

/*
* 更新文档数据（局部更新）
* param  index  索引库名称
* param  docid  文档ID
* param  data   数据结构体
* return ES响应结果map体
 */
func (etx EsContext) UpdateData(index string, documentType interface{}, docid string, data interface{}) (result interface{}, err error) {
	if docid == "" {
		return nil, errors.New("The docid cannot be empty")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		reqdata, err := json.MarshalToString(data)
		if err != nil {
			return nil, err
		}

		var b strings.Builder
		b.WriteString(`{"doc":`)
		b.WriteString(reqdata)
		b.WriteString("}")

		req := esapi6.UpdateRequest{
			Index:        index,
			DocumentType: _type,
			DocumentID:   docid,
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error UpdateData req=%s ", res.Status(), b.String()))
		} else {
			//res.Status(), r["result"], int(r["_version"].(float64)
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 删除文档数据
* param  index  索引库名称
* param  docid  文档ID
* return ES响应结果map体
 */
func (etx EsContext) DeleteData(index string, documentType interface{}, docid string) (result interface{}, err error) {
	if docid == "" {
		return nil, errors.New("The docid cannot be empty")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		req := esapi6.DeleteRequest{
			Index:        index,
			DocumentType: _type,
			DocumentID:   docid,
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error DeleteData", res.Status()))
		} else {
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 批量插入文档数据
* param  index   索引库名称
* param  docids  文档ID（数组数量要与datas的个数一致，数组成员为空字符串代表由系统生成ID）
* param  datas   数据结构体（支持N个批量插入），标签json标识对应字段名，结构体类型举例：
*    type User struct {
*        Userid uint32  `json:"user_id"`
*    	 Amount float64 `json:"amount"`
*    }
* return ES响应结果map体
 */
func (etx EsContext) BatchInsertData(index string, documentType interface{}, docids []string, datas ...interface{}) (result interface{}, err error) {
	if len(datas) == 0 {
		return nil, errors.New("The datas cannot be empty ")
	}
	if len(docids) != len(datas) {
		return nil, errors.New("The docids and datas length not the same ")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		var b strings.Builder

		for _i, data := range datas {
			/*dataStruct := reflect.Indirect(reflect.ValueOf(data))
			_valid := dataStruct.FieldByName("Docid")
			_docid := ""
			if _valid.IsValid() {
				_docid = _valid.String()
			}*/

			_docid := docids[_i]

			if _docid != "" {
				b.WriteString(fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}`, index, _type, _docid))
				b.WriteString("\n")
			} else {
				b.WriteString(fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s"}}`, index, _type))
				b.WriteString("\n")
			}

			reqdata, err := json.MarshalToString(data)
			if err != nil {
				return nil, err
			}

			b.WriteString(reqdata)
			b.WriteString("\n")
		}

		req := esapi6.BulkRequest{
			Index:        index,
			DocumentType: _type,
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() || err != nil {
			return nil, errors.New(fmt.Sprintf("[%s] Error=%v BatchInsertData req=%s", res.Status(), result, b.String()))
		} else {
			retobj := map[string][]int{
				"creates": {},
				"updates": {},
				"fails":   {},
			}

			if result != nil {
				jsonret := result.(map[string]interface{})
				if _retitems, ok := jsonret["items"]; ok {
					retitems := _retitems.([]interface{})
					for _i := 0; _i < len(retitems); _i++ {
						retitem := retitems[_i].(map[string]interface{})
						inret := retitem["index"].(map[string]interface{})
						if inret["result"] == "created" {
							retobj["creates"] = append(retobj["creates"], _i)
						} else if inret["result"] == "updated" {
							retobj["updates"] = append(retobj["updates"], _i)
						} else if inret["result"] == "noop" {
							retobj["updates"] = append(retobj["updates"], _i)
						} else {
							retobj["fails"] = append(retobj["fails"], _i)
						}
					}
				}
			}
			return retobj, nil
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 批量更新文档数据
* param  index   索引库名称
* param  docids  文档ID（数组数量要与datas的个数一致）
* param  datas   数据结构体（支持N个批量更新），标签json标识对应字段名，结构体类型举例：
*    type User struct {
*        Userid uint32  `json:"user_id"`
*    	 Amount float64 `json:"amount"`
*    }
* return ES响应结果map体
 */
func (etx EsContext) BatchUpdateData(index string, documentType interface{}, docids []string, datas ...interface{}) (result interface{}, err error) {
	if len(datas) == 0 {
		return nil, errors.New("The datas cannot be empty")
	}
	if len(docids) != len(datas) {
		return nil, errors.New("The docids and datas length not the same")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		var b strings.Builder

		for _i, data := range datas {
			_docid := docids[_i]
			if _docid == "" {
				return nil, errors.New("The docids cannot be empty")
			}

			b.WriteString(fmt.Sprintf(`{"update": {"_index": "%s", "_type": "%s", "_id": "%s"}}`, index, _type, _docid))
			b.WriteString("\n")

			reqdata, err := json.MarshalToString(data)
			if err != nil {
				return nil, err
			}

			b.WriteString(fmt.Sprintf(`{"doc": %s}`, reqdata))
			b.WriteString("\n")
		}

		req := esapi6.BulkRequest{
			Index:        index,
			DocumentType: _type,
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() || err != nil {
			return nil, errors.New(fmt.Sprintf("[%s] Error=%v BatchUpdateData req=%s", res.Status(), result, b.String()))
		} else {
			retobj := map[string][]int{
				"updates": {},
				"fails":   {},
			}

			if result != nil {
				jsonret := result.(map[string]interface{})
				if _retitems, ok := jsonret["items"]; ok {
					retitems := _retitems.([]interface{})
					for _i := 0; _i < len(retitems); _i++ {
						retitem := retitems[_i].(map[string]interface{})
						inret := retitem["update"].(map[string]interface{})
						if inret["result"] == "updated" {
							retobj["updates"] = append(retobj["updates"], _i)
						} else if inret["result"] == "noop" {
							retobj["updates"] = append(retobj["updates"], _i)
						} else {
							retobj["fails"] = append(retobj["fails"], _i)
						}
					}
				}
			}
			return retobj, nil
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 批量删除文档数据
* param  index   索引库名称
* param  docids  文档ID（需要删除的文档）
* return ES响应结果map体
 */
func (etx EsContext) BatchDeleteData(index string, documentType interface{}, docids []string) (result interface{}, err error) {
	if len(docids) == 0 {
		return nil, errors.New("The docids cannot be empty")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		var b strings.Builder

		for _, _docid := range docids {
			if _docid == "" {
				return nil, errors.New("The docids cannot be empty")
			}

			b.WriteString(fmt.Sprintf(`{"delete": {"_index": "%s", "_type": "%s", "_id": "%s"}}`, index, _type, _docid))
			b.WriteString("\n")
		}

		req := esapi6.BulkRequest{
			Index:        index,
			DocumentType: _type,
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() || err != nil {
			return nil, errors.New(fmt.Sprintf("[%s] Error=%v BatchDeleteData req=%s", res.Status(), result, b.String()))
		} else {
			retobj := map[string][]int{
				"deletes": {},
				"fails":   {},
			}

			if result != nil {
				jsonret := result.(map[string]interface{})
				if _retitems, ok := jsonret["items"]; ok {
					retitems := _retitems.([]interface{})
					for _i := 0; _i < len(retitems); _i++ {
						retitem := retitems[_i].(map[string]interface{})
						inret := retitem["delete"].(map[string]interface{})
						if inret["result"] == "deleted" {
							retobj["deletes"] = append(retobj["deletes"], _i)
						} else {
							retobj["fails"] = append(retobj["fails"], _i)
						}
					}
				}
			}
			return retobj, nil
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 批量存储文档数据，包括增、删和改数据
* param  index   索引库名称
* param  datas   数据体
* return ES响应结果map体
 */
func (etx EsContext) BatchStoreData(index string, documentType interface{}, datas ...TypeBatchData) (result interface{}, err error) {
	if len(datas) == 0 {
		return nil, errors.New("The datas cannot be empty ")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		var b strings.Builder

		for _, data := range datas {
			_docid := data.Doc
			if data.Type != TInsert && _docid == "" {
				return nil, errors.New("The doc cannot be empty ")
			}

			if data.Type == TUpdate {
				b.WriteString(fmt.Sprintf(`{"update": {"_index": "%s", "_type": "%s", "_id": "%s"}}`, index, _type, _docid))
				b.WriteString("\n")

				reqdata, err := json.MarshalToString(data.Data)
				if err != nil {
					return nil, err
				}

				b.WriteString(fmt.Sprintf(`{"doc": %s}`, reqdata))
			} else if data.Type == TDelete {
				b.WriteString(fmt.Sprintf(`{"delete": {"_index": "%s", "_type": "%s", "_id": "%s"}}`, index, _type, _docid))
			} else if data.Type == TInsert {
				if _docid != "" {
					b.WriteString(fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}`, index, _type, _docid))
					b.WriteString("\n")
				} else {
					b.WriteString(fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s"}}`, index, _type))
					b.WriteString("\n")
				}

				reqdata, err := json.MarshalToString(data.Data)
				if err != nil {
					return nil, err
				}

				b.WriteString(reqdata)
			}

			b.WriteString("\n")
		}

		req := esapi6.BulkRequest{
			Index:        index,
			DocumentType: _type,
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() || err != nil {
			return nil, errors.New(fmt.Sprintf("[%s] Error=%v BatchStoreData req=%s", res.Status(), result, b.String()))
		} else {
			retobj := map[string][]int{
				"creates": {},
				"updates": {},
				"deletes": {},
				"fails":   {},
			}

			if result != nil {
				jsonret := result.(map[string]interface{})
				if _retitems, ok := jsonret["items"]; ok {
					retitems := _retitems.([]interface{})
					for _i := 0; _i < len(retitems); _i++ {
						retitem := retitems[_i].(map[string]interface{})
						var inret map[string]interface{}
						if _inret, ok := retitem["index"]; ok {
							inret = _inret.(map[string]interface{})
						} else if _inret, ok := retitem["update"]; ok {
							inret = _inret.(map[string]interface{})
						} else if _inret, ok := retitem["delete"]; ok {
							inret = _inret.(map[string]interface{})
							if inret["result"]=="not_found" {
								inret["result"] = "deleted"
							}
						}
						if err, ok := inret["error"]; ok {
							logger.Warnf("eserror data:%v err:%v", datas[_i].Data, err)
						}

						if inret["result"] == "created" {
							retobj["creates"] = append(retobj["creates"], _i)
						} else if inret["result"] == "updated" {
							retobj["updates"] = append(retobj["updates"], _i)
						} else if inret["result"] == "deleted" {
							retobj["deletes"] = append(retobj["deletes"], _i)
						} else if inret["result"] == "noop" {
							retobj["updates"] = append(retobj["updates"], _i)
						} else {
							retobj["fails"] = append(retobj["fails"], _i)
						}
					}
				}
			}
			return retobj, nil
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 根据文档ID检索文档数据
* param  indexs   索引库名称，数组
* param  docids   文档ID
* return ES响应结果map体
 */
func (etx EsContext) QueryDataById(indexs []string, documentType interface{}, docids []string) (result interface{}, err error) {
	if len(docids) == 0 {
		return nil, errors.New("The docids cannot be empty ")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		query := map[string]map[string]interface{}{
			"ids": {
				"type":   _type,
				"values": docids,
			},
		}

		var b strings.Builder
		b.WriteString(`{"from": 0, "size": 10000,`)

		reqdata, err := json.MarshalToString(query)
		if err != nil {
			return nil, err
		}

		b.WriteString(fmt.Sprintf(`"query": %s}`, reqdata))

		res, err := etx.es.Search(
			etx.es.Search.WithContext(context.Background()),
			etx.es.Search.WithIndex(indexs...),
			etx.es.Search.WithBody(strings.NewReader(b.String())),
			etx.es.Search.WithTrackTotalHits(true),
		)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting response: %s", err))
		}

		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error QueryDataById, req:%s", res.Status(), b.String()))
		}
		if err != nil {
			return result, err
		}

		jsonobj, ok := result.(map[string]interface{})
		if !ok {
			return result, errors.New("Response error ")
		}

		return jsonobj["hits"], nil
	}

	return nil, errors.New("Elasticsearch init: fail ")
}

/*
* 检索文档数据
* param  indexs    索引库名称，数组
* param  musts     与条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：match（模糊匹配）, term（全匹配）, terms（相当于DB的in查询）, match_not（模糊不等于）, term_not（全不等于）, terms_not（相当于DB的not in查询）
*                 - key为字段名，value为检索值
* param  shoulds   或条件检索map体（结构如上musts）
* param  filters   过滤条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：wildcard（通配符匹配，检索值中带*代表通配）, regexp（正则表达式匹配）, prefix（前辍匹配）
*                 - key为字段名，value为检索值
* param  ranges    范围条件检索map体
*                 - map体对应的JSON格式为： {"key":{"type":"value"}}
*                 - type取值范围为：gte（大于或等于）, lte（小于或等于）, 其它（详见官方文档：https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-range-query.html）
*                 - key为字段名，value为检索值
* param  offset   记录开始序号（0开始）
* param  limit    返回记录条数
* param  sorts    排序map体
*                 - map体对应的JSON格式为： {"key":"type"}
*                 - key为字段名，type取值范围为：desc（降序）, asc（升序）
* param  source   要返回的字段，不传返回全部
* return ES响应结果map体
 */
func (etx EsContext) QueryData(indexs []string, musts map[string]map[string]interface{}, shoulds map[string]map[string]interface{}, filters map[string]map[string]interface{}, ranges map[string]map[string]interface{}, offset uint32, limit uint32, sorts map[string]interface{}, source ...string) (result interface{}, err error) {
	result, _, err = etx.ScrollQuery(indexs, musts, shoulds, filters, ranges, offset, limit, sorts, 0, source...)
	return result, err
}

/*
* 检索文档数据
* param  indexs    索引库名称，数组
* param  musts     与条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：match（模糊匹配）, term（全匹配）, terms（相当于DB的in查询）, match_not（模糊不等于）, term_not（全不等于）, terms_not（相当于DB的not in查询）
*                 - key为字段名，value为检索值
* param  shoulds   或条件检索map体（结构如上musts）
* param  filters   过滤条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：wildcard（通配符匹配，检索值中带*代表通配）, regexp（正则表达式匹配）, prefix（前辍匹配）
*                 - key为字段名，value为检索值
* param  ranges    范围条件检索map体
*                 - map体对应的JSON格式为： {"key":{"type":"value"}}
*                 - type取值范围为：gte（大于或等于）, lte（小于或等于）, 其它（详见官方文档：https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-range-query.html）
*                 - key为字段名，value为检索值
* param  offset   记录开始序号（0开始）
* param  limit    返回记录条数
* param  sorts    排序map体
*                 - map体对应的JSON格式为： {"key":"type"}
*                 - key为字段名，type取值范围为：desc（降序）, asc（升序）
* param  scroll   启用游标时间间隔（0为不启用）
* param  source   要返回的字段，不传返回全部
* return ES响应结果map体
 */
func (etx EsContext) ScrollQuery(indexs []string, musts map[string]map[string]interface{}, shoulds map[string]map[string]interface{}, filters map[string]map[string]interface{}, ranges map[string]map[string]interface{}, offset uint32, limit uint32, sorts map[string]interface{}, scroll time.Duration, source ...string) (result interface{}, scrollId string, err error) {
	queryStr, err := getQueryString(musts, shoulds, filters, ranges)
	if err != nil {
		return nil, "", err
	}

	if etx.es != nil {
		var sortobj []map[string]interface{}
		if sorts != nil {
			for _key, _type := range sorts {
				sortobj = append(sortobj, map[string]interface{}{
					_key: map[string]interface{}{
						"order": _type,
					},
				})
			}
		}

		var b strings.Builder
		b.WriteString("{")
		if len(source) > 0 {
			b.WriteString(`"_source": [`)
			for i, feild := range source {
				b.WriteString(`"`)
				b.WriteString(feild)
				b.WriteString(`"`)
				if i < (len(source) - 1) {
					b.WriteString(",")
				}
			}
			b.WriteString(` ],`)
		}
		b.WriteString(fmt.Sprintf(`"from": %d, "size": %d,`, offset, limit))

		if len(sortobj) != 0 {
			reqdata, err := json.MarshalToString(sortobj)
			if err != nil {
				return nil, "", err
			}
			b.WriteString(fmt.Sprintf(`"sort": %s,`, reqdata))
		}

		b.WriteString(queryStr)
		b.WriteString("}")

		res, err := etx.es.Search(
			etx.es.Search.WithContext(context.Background()),
			etx.es.Search.WithIndex(indexs...),
			etx.es.Search.WithBody(strings.NewReader(b.String())),
			etx.es.Search.WithTrackTotalHits(true),
			etx.es.Search.WithScroll(scroll),
		)
		if err != nil {
			return nil, "", errors.New(fmt.Sprintf("Error getting response: %s", err))
		}

		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, "", errors.New(fmt.Sprintf("[%s] Error QueryData, req:%s", res.Status(), b.String()))
		}
		if err != nil {
			return result, "", err
		}

		jsonobj, ok := result.(map[string]interface{})
		if !ok {
			return result, "", errors.New("Response error ")
		}

		if _scrollId, ok := jsonobj["_scroll_id"]; ok {
			return jsonobj["hits"], _scrollId.(string), nil
		}

		return jsonobj["hits"], "", nil
	}

	return nil, "", errors.New("Elasticsearch init: fail ")
}

/*
* 游标接口
* param  scrollId 游标ID
* param  scroll   启用游标时间间隔
 */
func (etx EsContext) PageByScroll(scrollId string, scroll time.Duration) (result interface{}, scrollId2 string, err error) {
	if etx.es != nil {
		res, err := etx.es.Scroll(
			etx.es.Scroll.WithScroll(scroll),
			etx.es.Scroll.WithScrollID(scrollId),
		)
		if err != nil {
			return nil, "", errors.New(fmt.Sprintf("Error getting response: %s", err))
		}

		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, "", errors.New(fmt.Sprintf("[%s] Error PageByScroll, scroll_id:%s", res.Status(), scrollId))
		}
		if err != nil {
			return result, "", err
		}

		jsonobj, ok := result.(map[string]interface{})
		if !ok {
			return result, "", errors.New("Response error ")
		}

		if _scrollId, ok := jsonobj["_scroll_id"]; ok {
			return jsonobj["hits"], _scrollId.(string), nil
		}

		return jsonobj["hits"], "", nil
	}

	return nil, "", errors.New("Elasticsearch init: fail ")
}

/*
* 通过检索文档进行删除数据
* param  indexs    索引库名称，数组
* param  musts     与条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：match（模糊匹配）, term（全匹配）, terms（相当于DB的in查询）, match_not（模糊不等于）, term_not（全不等于）, terms_not（相当于DB的not in查询）
*                 - key为字段名，value为检索值
* param  shoulds   或条件检索map体（结构如上musts）
* param  filters   过滤条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：wildcard（通配符匹配，检索值中带*代表通配）, regexp（正则表达式匹配）, prefix（前辍匹配）
*                 - key为字段名，value为检索值
* param  ranges    范围条件检索map体
*                 - map体对应的JSON格式为： {"key":{"type":"value"}}
*                 - type取值范围为：gte（大于或等于）, lte（小于或等于）, 其它（详见官方文档：https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-range-query.html）
*                 - key为字段名，value为检索值
* return ES响应结果map体
 */
func (etx EsContext) DeleteDataByQuery(indexs []string, documentType interface{}, musts map[string]map[string]interface{}, shoulds map[string]map[string]interface{}, filters map[string]map[string]interface{}, ranges map[string]map[string]interface{}) (result interface{}, err error) {
	queryStr, err := getQueryString(musts, shoulds, filters, ranges)
	if err != nil {
		return nil, err
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		var b strings.Builder

		b.WriteString(`{`)
		b.WriteString(queryStr)
		b.WriteString("}")

		req := esapi6.DeleteByQueryRequest{
			Index:        indexs,
			DocumentType: []string{_type},
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error DeleteDataByQuery req=%s", res.Status(), b.String()))
		} else {
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 通过分词检索删除文档数据
* param  indexs    索引库名称，数组
* param  queryString  检索map体(Query string query)
*                 - map体对应的JSON格式为： {"type":"value"}
*                 - type取值范围为：query（检索字符串）, fields（字段名列表）, default_field（默认字段名）
*                 - value为设置内容
* param  matchPhrase   检索map体（Match phrase query）
*                 - map体对应的JSON格式为： {"key":"value"}
*                 - key为字段名，value为检索值
* return ES响应结果map体
 */
func (etx EsContext) DeletePhraseString(indexs []string, documentType interface{}, queryString map[string]interface{}, matchPhrase map[string]string) (result interface{}, err error) {
	if queryString == nil && matchPhrase == nil {
		return nil, errors.New("Query parameter cannot be empty ")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		var b strings.Builder
		b.WriteString("{")
		b.WriteString(`"query": {`)

		if queryString != nil {
			reqQStr, err := json.MarshalToString(queryString)
			if err != nil {
				return nil, err
			}
			b.WriteString(fmt.Sprintf(`"query_string": %s`, reqQStr))
		}
		if matchPhrase != nil {
			reqPhrase, err := json.MarshalToString(matchPhrase)
			if err != nil {
				return nil, err
			}
			if queryString != nil {
				b.WriteString(",")
			}
			b.WriteString(fmt.Sprintf(`"match_phrase": %s`, reqPhrase))
		}
		b.WriteString("}}")

		req := esapi6.DeleteByQueryRequest{
			Index:        indexs,
			DocumentType: []string{_type},
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error DeleteDataByQuery req=%s", res.Status(), b.String()))
		} else {
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail ")
}

/*
* 通过检索文档进行修改数据
* param  indexs    索引库名称，数组
* param  updatas   修改数据map体，格式为：{"key":"value"}
* param  musts     与条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：match（模糊匹配）, term（全匹配）, terms（相当于DB的in查询）, match_not（模糊不等于）, term_not（全不等于）, terms_not（相当于DB的not in查询）
*                 - key为字段名，value为检索值
* param  shoulds   或条件检索map体（结构如上musts）
* param  filters   过滤条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：wildcard（通配符匹配，检索值中带*代表通配）, regexp（正则表达式匹配）, prefix（前辍匹配）
*                 - key为字段名，value为检索值
* param  ranges    范围条件检索map体
*                 - map体对应的JSON格式为： {"key":{"type":"value"}}
*                 - type取值范围为：gte（大于或等于）, lte（小于或等于）, 其它（详见官方文档：https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-range-query.html）
*                 - key为字段名，value为检索值
* return ES响应结果map体
 */
func (etx EsContext) UpdateDataByQuery(indexs []string, documentType interface{}, updatas map[string]interface{}, musts map[string]map[string]interface{}, shoulds map[string]map[string]interface{}, filters map[string]map[string]interface{}, ranges map[string]map[string]interface{}) (result interface{}, err error) {
	queryStr, err := getQueryString(musts, shoulds, filters, ranges)
	if err != nil {
		return nil, err
	}
	if updatas == nil {
		return nil, errors.New("No fields are provided to update ")
	}

	if etx.es != nil {
		_type := "_doc"
		if documentType != nil {
			_type = documentType.(string)
		}

		var b strings.Builder
		b.WriteString(`{`)
		b.WriteString(`"script": {"source": "`)

		isfirst := true
		var _ps []string
		for _key, _val := range updatas {
			if !isfirst {
				b.WriteString(`;`)
			}

			switch _val.(type) {
			case string:
				_val = strconv.Quote(_val.(string))
				_ps = append(_ps, fmt.Sprintf(`"%s":%s`, _key, _val))
			default:
				_ps = append(_ps, fmt.Sprintf(`"%s":%v`, _key, _val))
			}

			b.WriteString(fmt.Sprintf(`ctx._source['%s']=params['%s']`, _key, _key))

			isfirst = false
		}

		b.WriteString(`","params":{`)
		b.WriteString(strings.Join(_ps, ","))
		b.WriteString(`},"lang": "painless"},`)

		b.WriteString(queryStr)
		b.WriteString("}")

		req := esapi6.UpdateByQueryRequest{
			Index:        indexs,
			DocumentType: []string{_type},
			Body:         strings.NewReader(b.String()),
		}

		res, err := req.Do(context.Background(), etx.es)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error UpdateDataByQuery req=%s", res.Status(), b.String()))
		} else {
			return result, err
		}
	}

	return nil, errors.New("Elasticsearch init: fail")
}

/*
* 通过检索文档进行统计数据分析
* param  indexs    索引库名称，数组
* param  musts     与条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：match（模糊匹配）, term（全匹配）, terms（相当于DB的in查询）, match_not（模糊不等于）, term_not（全不等于）, terms_not（相当于DB的not in查询）
*                 - key为字段名，value为检索值
* param  shoulds   或条件检索map体（结构如上musts）
* param  filters   过滤条件检索map体
*                 - map体对应的JSON格式为： {"type":{"key":"value"}}
*                 - type取值范围为：wildcard（通配符匹配，检索值中带*代表通配）, regexp（正则表达式匹配）, prefix（前辍匹配）
*                 - key为字段名，value为检索值
* param  ranges    范围条件检索map体
*                 - map体对应的JSON格式为： {"key":{"type":"value"}}
*                 - type取值范围为：gte（大于或等于）, lte（小于或等于）, 其它（详见官方文档：https://www.elastic.co/guide/en/elasticsearch/reference/6.8/query-dsl-range-query.html）
*                 - key为字段名，value为检索值
* param  groupby   分组字段名数组
* param  columnby  分组统计项map体
*                 - map体对应的JSON格式为： {"name":{"type":"key"}}
*                 - name自定义统计项名称，key为字段名。一个name只允许有一个type和key
*                 - type取值范围为：sum（求和）, max（求最大值）, min（求最小值）, avg（求平均值）, value_count（求记录数）, 其它（详见官方文档：https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-aggregations-metrics.html）
* return ES响应结果map体
 */
func (etx EsContext) CountDataByQuery(indexs []string, musts map[string]map[string]interface{}, shoulds map[string]map[string]interface{}, filters map[string]map[string]interface{}, ranges map[string]map[string]interface{}, groupby []string, columnby map[string]map[string]interface{}) (result interface{}, err error) {
	if len(groupby) == 0 {
		return nil, errors.New("The groupby cannot be empty ")
	}

	queryStr, err := getQueryString(musts, shoulds, filters, ranges)
	if err != nil {
		return nil, err
	}

	if etx.es != nil {
		columnobj := make(map[string]map[string]interface{})
		if columnby != nil {
			for _name, _item := range columnby {
				for _type, _key := range _item {
					columnobj[_name] = map[string]interface{}{
						_type: map[string]interface{}{
							"field": _key,
						},
					}
				}
			}
		}

		var b strings.Builder

		b.WriteString(`{"from": 0, "size": 0`)

		for _, _by := range groupby {
			b.WriteString(fmt.Sprintf(`,"aggs": {"_%s_": {"terms": {"size": 10000, "field": "%s"}`, _by, _by))
		}

		if len(columnobj) != 0 {
			reqdata, err := json.MarshalToString(columnobj)
			if err != nil {
				return nil, err
			}
			b.WriteString(fmt.Sprintf(`,"aggs": %s`, reqdata))
		}

		for _i := 0; _i < len(groupby); _i++ {
			b.WriteString("}}")
		}

		b.WriteString(",")
		b.WriteString(queryStr)
		b.WriteString("}")

		res, err := etx.es.Search(
			etx.es.Search.WithContext(context.Background()),
			etx.es.Search.WithIndex(indexs...),
			etx.es.Search.WithBody(strings.NewReader(b.String())),
			etx.es.Search.WithTrackTotalHits(false),
			etx.es.Search.WithSource("false"),
		)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting response: %s", err))
		}

		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, errors.New(fmt.Sprintf("[%s] Error CountDataByQuery, req:%s", res.Status(), b.String()))
		}
		if err != nil {
			return result, err
		}

		jsonobj, ok := result.(map[string]interface{})
		if !ok {
			return result, errors.New("Response error ")
		}

		aggret := jsonobj["aggregations"].(map[string]interface{})
		_glen := len(groupby)
		viadata := make(map[string]interface{})

		retarr := getCountResult(aggret, viadata, groupby, 0, _glen, columnby)
		return retarr, nil
	}

	return nil, errors.New("Elasticsearch init: fail ")
}

/*
* 分词检索文档数据
* param  indexs    索引库名称，数组
* param  queryString  检索map体(Query string query)
*                 - map体对应的JSON格式为： {"type":"value"}
*                 - type取值范围为：query（检索字符串）, fields（字段名列表）, default_field（默认字段名）
*                 - value为设置内容
* param  matchPhrase   检索map体（Match phrase query）
*                 - map体对应的JSON格式为： {"key":"value"}
*                 - key为字段名，value为检索值
* param  offset   记录开始序号（0开始）
* param  limit    返回记录条数
* param  sorts    排序map体
*                 - map体对应的JSON格式为： {"key":"type"}
*                 - key为字段名，type取值范围为：desc（降序）, asc（升序）
* param  scroll   启用游标时间间隔（0为不启用）
* param  source   要返回的字段，不传返回全部
* return ES响应结果map体
 */
func (etx EsContext) QueryPhraseString(indexs []string, queryString map[string]interface{}, matchPhrase map[string]string, offset uint32, limit uint32, sorts map[string]interface{}, scroll time.Duration, source ...string) (result interface{}, scrollId string, err error) {
	if queryString == nil && matchPhrase == nil {
		return nil, "", errors.New("Query parameter cannot be empty ")
	}

	if etx.es != nil {
		var sortobj []map[string]interface{}
		if sorts != nil {
			for _key, _type := range sorts {
				sortobj = append(sortobj, map[string]interface{}{
					_key: map[string]interface{}{
						"order": _type,
					},
				})
			}
		}

		var b strings.Builder
		b.WriteString("{")
		if len(source) > 0 {
			b.WriteString(`"_source": [`)
			for i, feild := range source {
				b.WriteString(`"`)
				b.WriteString(feild)
				b.WriteString(`"`)
				if i < (len(source) - 1) {
					b.WriteString(",")
				}
			}
			b.WriteString(` ],`)
		}
		b.WriteString(fmt.Sprintf(`"from": %d, "size": %d,`, offset, limit))

		if len(sortobj) != 0 {
			reqdata, err := json.MarshalToString(sortobj)
			if err != nil {
				return nil, "", err
			}
			b.WriteString(fmt.Sprintf(`"sort": %s,`, reqdata))
		}

		b.WriteString(`"query": {`)
		if queryString != nil {
			reqQStr, err := json.MarshalToString(queryString)
			if err != nil {
				return nil, "", err
			}
			b.WriteString(fmt.Sprintf(`"query_string": %s`, reqQStr))
		}
		if matchPhrase != nil {
			reqPhrase, err := json.MarshalToString(matchPhrase)
			if err != nil {
				return nil, "", err
			}
			if queryString != nil {
				b.WriteString(",")
			}
			b.WriteString(fmt.Sprintf(`"match_phrase": %s`, reqPhrase))
		}
		b.WriteString("}}")

		res, err := etx.es.Search(
			etx.es.Search.WithContext(context.Background()),
			etx.es.Search.WithIndex(indexs...),
			etx.es.Search.WithBody(strings.NewReader(b.String())),
			etx.es.Search.WithTrackTotalHits(true),
			etx.es.Search.WithScroll(scroll),
		)
		if err != nil {
			return nil, "", errors.New(fmt.Sprintf("Error getting response: %s", err))
		}

		defer res.Body.Close()

		result, err := httpclient.GetResponse(res.Body, true)
		if res.IsError() {
			return result, "", errors.New(fmt.Sprintf("[%s] Error QueryData, req:%s", res.Status(), b.String()))
		}
		if err != nil {
			return result, "", err
		}

		jsonobj, ok := result.(map[string]interface{})
		if !ok {
			return result, "", errors.New("Response error ")
		}

		if _scrollId, ok := jsonobj["_scroll_id"]; ok {
			return jsonobj["hits"], _scrollId.(string), nil
		}

		return jsonobj["hits"], "", nil
	}

	return nil, "", errors.New("Elasticsearch init: fail ")
}

/*
* 内部方法：组装查询条件字符串
 */
func getQueryString(musts map[string]map[string]interface{}, shoulds map[string]map[string]interface{}, filters map[string]map[string]interface{}, ranges map[string]map[string]interface{}) (string, error) {
	var mustnotobj []map[string]interface{}
	var mustobj []map[string]interface{}
	if musts != nil {
		for _type, _item := range musts {
			for _key, _val := range _item {
				if _type == "term_not" || _type == "terms_not" || _type == "match_not" {
					tmptype := strings.Split(_type, "_")
					mustnotobj = append(mustnotobj, map[string]interface{}{
						tmptype[0]: map[string]interface{}{
							_key: _val,
						},
					})
				} else {
					mustobj = append(mustobj, map[string]interface{}{
						_type: map[string]interface{}{
							_key: _val,
						},
					})
				}
			}
		}
	}

	var shouldobj []map[string]interface{}
	if shoulds != nil {
		for _type, _item := range shoulds {
			for _key, _val := range _item {
				shouldobj = append(shouldobj, map[string]interface{}{
					_type: map[string]interface{}{
						_key: _val,
					},
				})
			}
		}
	}

	var filterobj []map[string]interface{}
	if filters != nil {
		for _type, _item := range filters {
			for _key, _val := range _item {
				filterobj = append(filterobj, map[string]interface{}{
					_type: map[string]interface{}{
						_key: _val,
					},
				})
			}
		}
	}

	if ranges != nil {
		for _key, _item := range ranges {
			_orderby := make(map[string]interface{}, 0)
			for _type, _val := range _item {
				_orderby[_type] = _val
			}

			filterobj = append(filterobj, map[string]interface{}{
				"range": map[string]map[string]interface{}{
					_key: _orderby,
				},
			})
		}
	}

	var rootJson strings.Builder
	var boolJson strings.Builder
	var isfrist bool = true

	rootJson.WriteString(`"query": {`)

	if len(mustobj) != 0 {
		if !isfrist {
			boolJson.WriteString(",")
		}

		reqdata, err := json.MarshalToString(mustobj)
		if err != nil {
			return "", err
		}
		boolJson.WriteString(fmt.Sprintf(`"must": %s`, reqdata))

		isfrist = false
	}

	if len(mustnotobj) != 0 {
		if !isfrist {
			boolJson.WriteString(",")
		}

		reqdata, err := json.MarshalToString(mustnotobj)
		if err != nil {
			return "", err
		}
		boolJson.WriteString(fmt.Sprintf(`"must_not": %s`, reqdata))

		isfrist = false
	}

	if len(shouldobj) != 0 {
		if !isfrist {
			boolJson.WriteString(",")
		}

		reqdata, err := json.MarshalToString(shouldobj)
		if err != nil {
			return "", err
		}
		boolJson.WriteString(fmt.Sprintf(`"should": %s`, reqdata))

		isfrist = false
	}

	if len(filterobj) != 0 {
		if !isfrist {
			boolJson.WriteString(",")
		}

		reqdata, err := json.MarshalToString(filterobj)
		if err != nil {
			return "", err
		}
		boolJson.WriteString(fmt.Sprintf(`"filter": %s`, reqdata))

		isfrist = false
	}

	if boolJson.Len() != 0 {
		rootJson.WriteString(`"bool": {`)
		rootJson.WriteString(boolJson.String())
		rootJson.WriteString(`}`)
	}

	rootJson.WriteString("}")

	if boolJson.Len() == 0 {
		return "", errors.New("Query parameter cannot be empty ")
	}

	return rootJson.String(), nil
}

//内部方法：遍历获取统计结果
func getCountResult(jsonret map[string]interface{}, viadata map[string]interface{}, groupby []string, _i int, _glen int, columnby map[string]map[string]interface{}) (datas []interface{}) {
	_by := groupby[_i]
	tempkey := fmt.Sprintf("_%s_", _by)

	if _levelobj, ok := jsonret[tempkey]; ok {
		levelobj := _levelobj.(map[string]interface{})
		buckets := levelobj["buckets"].([]interface{})

		for _, _bucket := range buckets {
			bucket := _bucket.(map[string]interface{})

			viadata[_by] = bucket["key"]
			if _i < (_glen - 1) {
				tmpdatas := getCountResult(bucket, viadata, groupby, _i+1, _glen, columnby)
				datas = append(datas, tmpdatas...)
			} else {
				tmpdata := make(map[string]interface{})
				for _key, _val := range viadata {
					tmpdata[_key] = _val
				}
				for _key, _ := range columnby {
					_item := bucket[_key].(map[string]interface{})

					if _itemval, ok := _item["value"]; ok {
						tmpdata[_key] = _itemval
					} else if _childs, ok := _item["buckets"]; ok {
						childs := _childs.([]interface{})
						if len(childs) != 0 {
							_childitem := childs[0].(map[string]interface{})
							tmpdata[_key] = _childitem["key"]
						}
					}
				}

				datas = append(datas, tmpdata)
			}
		}
	}

	return
}
