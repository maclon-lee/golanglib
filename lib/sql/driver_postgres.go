package sql

import (
	_ "github.com/lib/pq"
	"xorm.io/xorm"
)

func NewPostgresDriver(dataSourceName string) (*xorm.Engine, error) {
	return xorm.NewEngine("postgres", dataSourceName)
}