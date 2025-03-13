package mongodb

import (
	"gmf/conf"
)

func init() {
	var (
		mgConf = &conf.Default().Mongo
	)
	Init(mgConf.Addr(), mgConf.DBName, mgConf.User, mgConf.Pwd)
}
