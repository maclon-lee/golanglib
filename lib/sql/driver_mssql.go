package sql

import (
	_ "github.com/denisenkom/go-mssqldb"
	"xorm.io/xorm"
)

func NewMssqlDriver(dataSourceName string) (*xorm.Engine, error) {
	return xorm.NewEngine("mssql", dataSourceName)
}