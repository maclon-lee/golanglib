package client

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/maclon-lee/golanglib/lib/json"
	jsonrpc "github.com/maclon-lee/golanglib/lib/rpc/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

/*
* JSON RPC 统一输出错误
 */
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return e.Message
}

/*
* Http响应body体接收处理
 */
func GetResponse(body io.ReadCloser, isjson bool) (result interface{}, err error) {
	if body != nil && body != http.NoBody {
		resbyte, _ := ioutil.ReadAll(body)

		if resbyte != nil {
			if isjson {
				tobj := make(map[string]interface{})
				err := json.Unmarshal(resbyte, &tobj)
				if err != nil {
					return nil, errors.New(fmt.Sprintf("Error in gjson.Unmarshal %s => %s", string(resbyte), err))
				} else {
					return tobj, nil
				}
			} else {
				return string(resbyte), nil
			}
		}
	}

	return nil, errors.New("Response: no result")
}

/*
* 普通HTTP请求方法
*
* param  httpurl 请求URL
* param  pdata   请求数据体
* param  method  请求方式，POST,GET,PUT,DELETE
* param  header  请求头信息，格式为：map[string]string{}
* param  isjson  返回结果是否转换为JSON，true为map体，false为字符串
* return 返回结果
 */
func HttpRequest(httpurl string, pdata string, method string, header interface{}, isjson bool) (result interface{}, err error) {
	var req *http.Request

	reqdata := new(strings.Reader)
	if pdata != "" {
		reqdata = strings.NewReader(pdata)
	} else {
		reqdata = strings.NewReader("")
	}

	req, err = http.NewRequest(method, httpurl, reqdata)
	if err != nil {
		return nil, err
	}

	if header != nil {
		for _key, _val := range header.(map[string]string) {
			req.Header.Set(_key, _val)
		}
	}

	client := new(http.Client)
	timeout, _ := time.ParseDuration("30s")
	client.Timeout = timeout
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("response error, Status Code:%d", resp.StatusCode))
	}

	result, err = GetResponse(resp.Body, isjson)
	if err != nil {
		return nil, err
	}

	return result, nil
}

/*
* JSON RPC 请求方法
*
* param  httpurl 请求URL
* param  method  请求方法名
* param  args    请求参数列表，格式为：map[string]interface{}
* param  result  返回结果，JSON的map体
* return 错误信息
 */
func JsonRpcRequest(httpurl string, method string, args []interface{}, result interface{}) *RPCError {
	rpcErr := &RPCError{}

	message, err := jsonrpc.EncodeClientRequest(method, args)
	if err != nil {
		rpcErr.Code = 1900
		rpcErr.Message = fmt.Sprintf("jsonrpc.EncodeClientRequest %s %s err: %s", httpurl, method, err.Error())
		return rpcErr
	}

	req, err := http.NewRequest("POST", httpurl, bytes.NewBuffer(message))
	if err != nil {
		rpcErr.Code = 1900
		rpcErr.Message = fmt.Sprintf("http.NewRequest %s %s err: %s", httpurl, method, err.Error())
		return rpcErr
	}

	req.Header.Set("Content-Type", "application/json")
	client := new(http.Client)
	timeout, _ := time.ParseDuration("30s")
	client.Timeout = timeout
	resp, err := client.Do(req)
	if err != nil {
		rpcErr.Code = 1900
		rpcErr.Message = fmt.Sprintf("Error in sending request to %s. %s %s", httpurl, method, err.Error())
		return rpcErr
	}
	defer resp.Body.Close()

	jsonErr := new(interface{})
	err = jsonrpc.DecodeClientResponse(resp.Body, result, jsonErr)
	if err != nil {
		rpcErr.Code = 1900
		if err == jsonrpc.ErrNullResult {
			rpcErr.Code = 0
		} else {
			rpcErr.Message = err.Error()
		}

		if *jsonErr != nil {
			jerr, ok := (*jsonErr).(map[string]interface{})
			if !ok {
				return rpcErr
			}

			if _, ok := jerr["code"]; ok {
				rpcErr.Code = int(jerr["code"].(float64))
				rpcErr.Message = jerr["message"].(string)
			}
		}

		return rpcErr
	}

	return nil
}

/*
* 普通HTTP代理请求方法
*
* param  httpurl    请求URL
* param  pdata  请求数据体
* param  method 请求方式，POST,GET,PUT,DELETE
* param  header 请求头信息，格式为：map[string]string{}
* param  isjson 返回结果是否转换为JSON，true为map体，false为字符串
* return 返回结果
 */
func HttpProxyRequest(httpurl string, pdata string, method string, header interface{}, isjson bool, proxyIp string, proxyUserName string, proxyPwd string) (result interface{}, err error) {
	var req *http.Request

	reqdata := new(strings.Reader)
	if pdata != "" {
		reqdata = strings.NewReader(pdata)
	} else {
		reqdata = strings.NewReader("")
	}

	req, err = http.NewRequest(method, httpurl, reqdata)
	if err != nil {
		return nil, err
	}

	if header != nil {
		for _key, _val := range header.(map[string]string) {
			req.Header.Set(_key, _val)
		}
	}

	httpuri, _ := url.Parse(httpurl)
	proxyuri, _ := url.Parse(fmt.Sprintf("%s://%s:%s@%s", httpuri.Scheme, proxyUserName, proxyPwd, proxyIp))

	transProxy := &http.Transport{
		Proxy:                  http.ProxyURL(proxyuri),
		MaxIdleConns:           3,
		MaxIdleConnsPerHost:    3,
		IdleConnTimeout:        time.Duration(30) * time.Second,
	}

	client := new(http.Client)
	timeout := time.Duration(30) * time.Second
	client.Transport = transProxy
	client.Timeout = timeout
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("response error, Status Code:%d", resp.StatusCode))
	}

	result, err = GetResponse(resp.Body, isjson)
	if err != nil {
		return nil, err
	}

	return result, nil
}
