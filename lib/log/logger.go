package logger

import (
	"fmt"
	"github.com/maclon-lee/golanglib/lib/config"
	"github.com/maclon-lee/golanglib/lib/utility"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	syslog "log"
	"time"
)

var log *zap.SugaredLogger

//初始化
func init() {
	var cfg zap.Config
	if config.Env == "prod" {
		cfg = zap.NewProductionConfig()
		cfg.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	} else {
		cfg = zap.NewDevelopmentConfig()
	}
	cfg.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(utility.FORMATSQLTIME)

	path, _ := utility.GetPathValid("./log/", "../log/", "../../log/")

	filename := fmt.Sprintf("%s%s.log", path, time.Now().Format("20060102"))
	cfg.OutputPaths = []string{"stdout", filename}
	cfg.ErrorOutputPaths = []string{"stderr", filename}
	cfg.DisableCaller = true

	opt := zap.AddCallerSkip(1)
	zablog, err := cfg.Build(opt)
	if err != nil {
		syslog.Panic(err)
	}
	log = zablog.Sugar()
}

//调试日志
func Debugf(format string, a ...interface{}) {
	log.Debugf(format, a...)
}

//信息日志
func Infof(format string, a ...interface{}) {
	log.Infof(format, a...)
}

//警告日志
func Warnf(format string, a ...interface{}) {
	log.Warnf(format, a...)
	sendAliLog("Warn", format, a...)
}

//错误日志
func Errorf(format string, a ...interface{}) {
	log.Errorf(format, a...)
	sendAliLog("Error", format, a...)
	alarmByDingtalk(format, a...)
}
