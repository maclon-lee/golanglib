package config

import (
	"fmt"
	"github.com/spf13/viper"
)

var Env = "prod"

func init() {
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/conf/") // path to look for the config file in
	viper.AddConfigPath("./conf")
	viper.AddConfigPath("../conf")
	viper.AddConfigPath("../../conf")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s ", err))
	}
	Env = GetSubConfig("setting").GetString("env")
}

//根据配置节点key获取配置的map，key==""时获取所有配置
func GetConfigMap(key string) map[string]interface{} {
	if key == "" {
		return viper.AllSettings()
	}
	return viper.GetStringMap(key)
}

//获取子节点的配置，如kafka，mysql，sqlserver...
func GetSubConfig(key string) *viper.Viper {
	return viper.Sub(key)
}

//是否配置了子节点
func IsSet(key string) bool {
	return viper.IsSet(key)
}

//获取子节点配置，对象化
func UnmarshalKey(key string, rawVal interface{}, opts ...viper.DecoderConfigOption) error {
	return viper.UnmarshalKey(key, rawVal, opts ...)
}
