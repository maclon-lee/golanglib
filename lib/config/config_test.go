package config

import "testing"

func TestGetSubConfig(t *testing.T) {
	root := GetSubConfig("dbs")
	keys := root.AllKeys()
	t.Log(keys)
	sub := root.Sub("db")
	setting := sub.AllSettings()
	t.Log(setting)

}
