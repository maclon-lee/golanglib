package httpd

import (
	"context"
	"fmt"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/maclon-lee/golanglib/lib/config"
	signer "github.com/maclon-lee/golanglib/lib/httpd/auth"
	logger "github.com/maclon-lee/golanglib/lib/log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

const (
	WebApiPort       = 80
	HttpReadTimeout  = 30
	HttpWriteTimeout = 60
)
var apiAuthUsers []*apiAuthUser
var authType = 0       // 0为basic验证(默认), 1为sha256验证
var isEncrypt = false  // 是否传输内容AES256加密，true为针对Body内容加密，false为不加密

// Httpd服务
type server struct {
	Router   func(*gin.Engine)                     // 路由
	Name     string                                // 服务名称
	Port     int                                   // 端口号，未指定时默认时80
	Stats    func(*gin.Context, interface{}) error // 扩展校验(角色/状态等)
	Throttle func() gin.HandlerFunc                // 限流(频率限制、协程数限制和超时限制)
}
//HTTP API帐号
type apiAuthUser struct {
	Username string `mapstructure:"basicAuthUsername"`
	Password string `mapstructure:"basicAuthUserPassword"`

	ShaAuthKey    string `mapstructure:"shaAuthKey"`
	ShaAuthSecret string `mapstructure:"shaAuthSecret"`
	ShaExpiration int    `mapstructure:"shaExpiration"`
}

/*
* 创建HTTP API服务
*
* param  appName  服务命名，便于输出日志
* param  port     侦听端口
* param  router   API路由注册回调函数
* param  stats    预留
* param  throttle 限流回调函数
 */
func NewHttpServer(appName string, port int, router func(*gin.Engine), stats func(*gin.Context, interface{}) error,
	throttle func() gin.HandlerFunc) server {
	if port == 0 {
		port = WebApiPort
	}
	return server{
		Name:     appName,
		Port:     port,
		Router:   router,
		Stats:    stats,
		Throttle: throttle,
	}
}

//启动http server的监听
func (this server) Start() {
	//加载鉴权配置
	if config.IsSet("http") {
		cnf := config.GetSubConfig("http")

		if cnf.IsSet("authType") {
			authType = cnf.GetInt("authType")
		}
		if cnf.IsSet("isEncrypt") {
			isEncrypt = cnf.GetBool("isEncrypt")
		}

		if cnf.IsSet("user") {
			err := cnf.UnmarshalKey("user", &apiAuthUsers)
			if err != nil {
				logger.Errorf("读取HTTP API鉴权账号失败")
			}
		}
		if cnf.IsSet("basicAuthUsername") && cnf.IsSet("basicAuthUserPassword") {
			apiAuthUsers = append(apiAuthUsers, &apiAuthUser{
				Username: cnf.GetString("basicAuthUsername"),
				Password: cnf.GetString("basicAuthUserPassword"),
			})
		}
	}

	// 构建GIN引擎;
	r := this.engine()
	// URL路由注册;
	this.Router(r)

	if config.Env != "prod" {
		// 开启pprof性能采集
		pprof.Register(r)
	}

	// 构建服务;
	s := &http.Server{
		Addr:         fmt.Sprintf(":%d", this.Port),
		Handler:      r,
		ReadTimeout:  HttpReadTimeout * time.Second,
		WriteTimeout: HttpWriteTimeout * time.Second,
	}
	// 启动服务;
	go func() {
		if err := s.ListenAndServe(); err != nil {
			logger.Errorf("启动服务:%v失败(:%v), err:%v.", this.Name, this.Port, err)
		} else {
			logger.Infof("启动服务:%v成功(:%v).", this.Name, this.Port)
		}
	}()

	// 信号通道(关闭服务);
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	// 关闭服务;
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// 热重启;
	if err := s.Shutdown(ctx); err != nil {
		logger.Errorf("关闭服务:%v失败(:%v), err:%v.", this.Name, this.Port, err)
	} else {
		logger.Infof("关闭服务:%v成功(:%v).", this.Name, this.Port)
	}
}

// 构建GIN引擎;
func (this server) engine() *gin.Engine {
	// 设置模式;
	mode := gin.ReleaseMode
	switch config.Env {
	case "dev":
		mode = gin.DebugMode
	case "stage":
		mode = gin.TestMode
	}
	gin.SetMode(mode)
	// 创建空的GIN引擎;
	e := gin.New()
	// K8S检测心跳(URL);
	e.Use(heartbeat())
	// CORS跨域;
	e.Use(cors())
	//接口校验
	e.Use(auth())
	// 捕获中断拦截器;
	e.Use(_recover())
	// 日志拦截;
	e.Use(logging())
	// API限流
	if this.Throttle != nil {
		e.Use(this.Throttle())
	}

	// 权限验证
	//e.Use(_permit(this.PermitType, this.Authr, this.Stats))
	// 链路追踪(opentarcing);
	//e.Use(tracing.Httpd().Middleware())
	// 普罗米修斯监控;
	//e.Use(metrics.Httpd(this.Named).Middleware())
	return e
}

//接口调用基本验证
func auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authPass := false
		if len(apiAuthUsers) > 0 {
			if authType == 0 {
				user, pwd, ok := c.Request.BasicAuth()
				if ok {
					for _, authUser := range apiAuthUsers {
						if authUser.Username == user && authUser.Password == pwd {
							authPass = true
							//可通过 user := c.GetString("AuthUserName") 获取当前请求用户名
							c.Set("AuthUserName", user)
							break
						}
					}
				}
			} else if authType == 1 {
				sr := &signer.Signer{}
				appKey, signedHeaders, token, err := sr.GetAuthHeader(c.Request)
				if err == nil {
					var user *apiAuthUser
					for _, authUser := range apiAuthUsers {
						if authUser.ShaAuthKey == appKey {
							user = authUser
							break
						}
					}

					if user != nil {
						sr.Key = user.ShaAuthKey
						sr.Secret = user.ShaAuthSecret

						expir, _ := time.ParseDuration(fmt.Sprintf("%dms", user.ShaExpiration))
						err = sr.Verify(c.Request, signedHeaders, token, expir)
						if err == nil {
							authPass = true
							//可通过 user := c.GetString("AuthUserName") 获取当前请求用户名
							c.Set("AuthUserName", user.ShaAuthKey)
						}

						//Body内容解密
						if authPass && isEncrypt {
							err = sr.DecryptPayload(c.Request)
							if err != nil {
								c.JSON(http.StatusBadRequest, gin.H{
									"status":  "unauthorized",
									"message": "传输内容解密失败",
								})
								c.Abort()
							}
						}
					}
				}
			}
		} else {
			authPass = true
		}

		if !authPass {
			c.JSON(http.StatusBadRequest, gin.H{
				"status":  "unauthorized",
				"message": "接口鉴权无效",
			})
			c.Abort()
		} else {
			c.Next()
		}
	}
}

//日志
func logging() gin.HandlerFunc {
	return func(c *gin.Context) {
		stamp := time.Now()
		c.Next()
		logger.Infof("调用结束(APIs %s), 耗时: %v.", c.Request.URL.Path, time.Since(stamp))
	}
}

//异常捕获
func _recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 捕获中断;
		defer func() {
			if err := recover(); err != nil {
				errMsg := fmt.Sprintf("%s", err)
				if strings.Index(errMsg, "i/o timeout. Response") != -1 || strings.Index(errMsg, "broken pipe. Response") != -1 {
					logger.Warnf("API请求错误, URL:%s, Error:%s.", c.FullPath(), errMsg)
				} else {
					logger.Errorf("API请求错误, URL:%s, Error:%s.", c.FullPath(), errMsg)
				}

				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	}
}

//跨域设置
func cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 支持所有irobotbox.com子域的跨域调用;
		c.Header("Access-Control-Allow-Origin", "*.irobotbox.com")
		c.Header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "*")

		// 校验请求;
		if c.Request.Method == "OPTIONS" {
			// 中止跳过, 响应码(200);
			c.AbortWithStatus(http.StatusOK)
		} else {
			c.Next()
		}
	}
}

// 检测心跳(URL);
func heartbeat() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 校验请求(路径);
		if strings.HasSuffix(c.Request.URL.Path, "/Heartbeat") {
			Ok(c, nil)
			c.AbortWithStatus(http.StatusOK)
		}
	}
}
