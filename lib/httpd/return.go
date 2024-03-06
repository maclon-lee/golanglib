package httpd

import (
	"github.com/gin-gonic/gin"
	"github.com/maclon-lee/golanglib/lib/json"
	"net/http"
	"time"
)

//统一返回结构
type Response struct {
	Timestamp time.Time   `json:"timestamp"`         //时间戳
	Status    string      `json:"status,omitempty"`  //接口请求的返回状态，success 或者 error
	Message   string      `json:"message,omitempty"` //返回消息
	Data      interface{} `json:"data,omitempty"`    //具体数据
}

// 分页结构体
type PagingResult struct {
	List       interface{} `json:"list,omitempty"`
	Pagination Pagination  `json:"pagination,omitempty"`
}
type Paging struct {
	Page     int `json:"page"`
	PageSize int `json:"pageSize"`
}
type Pagination struct {
	Current  int   `json:"current"`
	PageSize int   `json:"pageSize"`
	Total    int64 `json:"total,omitempty"`
}

//分页数据结构体
func NewPagingResult(current, pagesize int, total int64, list interface{}) PagingResult {
	return PagingResult{
		Pagination: Pagination{
			Current:  current,
			PageSize: pagesize,
			Total:    total,
		},
		List: list,
	}
}

// 返回成功 http 200
func Ok(c *gin.Context, data interface{}) {
	resp := &Response{
		Timestamp: time.Now(),
		Status:    "success",
		Message:   "success",
		Data:      data,
	}

	c.Abort()
	c.Render(http.StatusOK, json.JsoniterJSON{Data: resp})
}

// 返回失败 http 400
func Error(c *gin.Context, err error) {
	resp := &Response{
		Timestamp: time.Now(),
		Status:    "error",
		Message:   err.Error(),
	}

	c.Render(http.StatusBadRequest, json.JsoniterJSON{Data: resp})
}

//返回自定义 status, http 200
func CustomStatus(c *gin.Context, status, message string, data interface{}) {
	resp := &Response{
		Timestamp: time.Now(),
		Status:    status,
		Data:      data,
		Message:   message,
	}

	c.Render(http.StatusOK, json.JsoniterJSON{Data: resp})
}
