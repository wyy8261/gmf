package mongodb

import (
	"github.com/wyy8261/gmf/conf"
)

func init() {
	var (
		mgConf = &conf.Default().Mongo
	)
	Init(mgConf.Addr(), mgConf.DBName, mgConf.User, mgConf.Pwd)
}
