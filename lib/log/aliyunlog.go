package logger

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
	"github.com/maclon-lee/golanglib/lib/config"
	httpclient "github.com/maclon-lee/golanglib/lib/httpd/client"
	"github.com/maclon-lee/golanglib/lib/json"
	"github.com/maclon-lee/golanglib/lib/utility"
	syslog "log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type AlarmConfig struct {
	Enable  bool
	Url     string
	Secret  string
	Mobiles []string

	Time time.Time
	Num  int
}

var alarmCfg *AlarmConfig
var aliLogInstance *producer.Producer
var aliLogProjectName string
var aliLogStoreName string

type aliLogCallbackHandler struct{
}
func (aliLogCallbackHandler) Success(_ *producer.Result) {
	Infof("AliyunLog Success")
}
func (aliLogCallbackHandler) Fail(result *producer.Result) {
	Infof("AliyunLog Fail requestId:%s, errcode:%s, errmsg:%s", result.GetRequestId(), result.GetErrorCode(), result.GetErrorMessage())
}

//初始化
func init() {
	if config.Env == "prod" && config.IsSet("aliyunlog") && aliLogInstance == nil {
		cfglist := config.GetSubConfig("aliyunlog")

		logCfg := producer.GetDefaultProducerConfig()
		logCfg.Endpoint = cfglist.GetString("endpoint")
		logCfg.AccessKeyID = cfglist.GetString("accessKeyID")
		logCfg.AccessKeySecret = cfglist.GetString("accessKeySecret")
		logCfg.AllowLogLevel = "warn"
		logCfg.MaxBatchSize = 1024 * 16
		logCfg.MaxBatchCount = 2

		aliLogProjectName = cfglist.GetString("projectName")
		aliLogStoreName = cfglist.GetString("storeName")
		aliLogInstance = producer.InitProducer(logCfg)

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		aliLogInstance.Start()

		go func() {
		DoneLoop:
			for {
				select {
				case <-signals:
					aliLogInstance.Close(30000)
					break DoneLoop
				}
			}
		}()
	}

	//报警配置
	alarmCfg = &AlarmConfig{
		Enable: false,
		Num:    0,
	}
	if config.Env == "prod" && config.IsSet("dingtalk") {
		cnf := config.GetSubConfig("dingtalk")

		alarmCfg.Enable = cnf.GetBool("isOpen")
		alarmCfg.Url = cnf.GetString("url")
		alarmCfg.Secret = cnf.GetString("secret")
		alarmCfg.Mobiles = cnf.GetStringSlice("atMobiles")
	}
}

//内部方法：写入阿里云日志服务
func sendAliLog(level string, format string, v ...interface{}) {
	if aliLogInstance == nil {
		return
	}

	lf := 2
	var tracks []string
	for lf < 10 {
		_, file, line, ok := runtime.Caller(lf)
		if ok {
			tracks = append(tracks, fmt.Sprintf("%s:%d", file, line))
		}
		lf++
	}

	hostname, _ := os.Hostname()
	sourcename := fmt.Sprintf("%s(%s)", config.Env, hostname)

	loginfo := producer.GenerateLog(uint32(time.Now().Unix()), map[string]string{"Level": level, "File": tracks[0], "Info": fmt.Sprintf(format, v...), "Track": strings.Join(tracks, ", ")})
	topic := time.Now().Format(utility.FORMATDATE)
	callback := aliLogCallbackHandler{}

	err := aliLogInstance.SendLogWithCallBack(aliLogProjectName, aliLogStoreName, topic, sourcename, loginfo, callback)
	if err != nil {
		syslog.Printf("%s", err)
	}
}

//内部方法：钉钉报警
func alarmByDingtalk(format string, v ...interface{}) {
	if alarmCfg.Enable {
		alarmCfg.Num = alarmCfg.Num + 1
		if time.Now().Unix()-alarmCfg.Time.Unix() < 60 {
			return
		}

		defer func() {
			alarmCfg.Time = time.Now()
			alarmCfg.Num = 0
		}()

		bugfile := ""
		_, file, line, ok := runtime.Caller(2)
		if ok {
			bugfile = fmt.Sprintf("%s:%d", file, line)
		}
		errInfo := fmt.Sprintf(format, v...)
		if len(errInfo) > 200 {
			errInfo = errInfo[:200]
		}
		info := fmt.Sprintf("%s【%d条】%s %s", time.Now().Format(utility.FORMATLOGTIME), alarmCfg.Num, bugfile, errInfo)

		url := alarmCfg.Url
		if alarmCfg.Secret != "" {
			timestamp := time.Now().Unix() * 1000

			stringToSign := fmt.Sprintf("%d\n%s", timestamp, alarmCfg.Secret)
			hmac256 := hmac.New(sha256.New, []byte(alarmCfg.Secret))
			hmac256.Write([]byte(stringToSign))
			sha := hmac256.Sum(nil)
			sign := base64.StdEncoding.EncodeToString(sha)

			url = fmt.Sprintf("%s&timestamp=%d&sign=%s", alarmCfg.Url, timestamp, sign)
		}

		atMobiles := alarmCfg.Mobiles
		strmobis, err := json.MarshalToString(atMobiles)
		if err != nil {
			syslog.Printf("alarmByDingtalk error: %s", err.Error())
			return
		}

		header := map[string]string{}
		header["Content-Type"] = "application/json;charset=utf8"
		info = strconv.Quote(info)
		pdata := fmt.Sprintf(`{"msgtype": "text", "at": {"atMobiles": %s, "isAtAll": false}, "text": {"content": %s}}`, strmobis, info)
		syslog.Printf("%s", pdata)

		result, err := httpclient.HttpRequest(url, pdata, "POST", header, true)
		if err != nil {
			syslog.Printf("HttpRequest error: %s", err.Error())
			return
		}

		jsonret := result.(map[string]interface{})
		if jsonret["errcode"].(float64) != 0 {
			syslog.Printf("%v", result)
		}
	}
}